#########################################################
# PRACTICA ASO : Servidor web distribuido
# ---------------------------------------
# Fase actual : 8 (Coordinacion de actualizaciones)
# Fichero : separador.b
# Descripcion : Reparte las peticiones a uno u otro servidor, en funcion
#    			de su directorio
# Autor : David Rozas
#########################################################

implement repartidor;

repartidor: module
{
	PATH	: con "./repartidor.dis";

	init	: fn(nil: ref Draw->Context, argv: list of string);
};

include "sys.m";
	sys: Sys;
	fprint, fildes, FD, ORDWR, tokenize, open, dial, pctl, sprint,
	announce, listen, OWRITE, write: import sys;
include "draw.m";

include "string.m";
	str : String;

include "wp.m";
	wp : Wp;
	readmsg, Reqmsg, Repmsg: import wp;

#Constantes


#Definicion de tipos para almacenar info de servers
tServer : adt
{
	direccion : string;
	conexion : ref FD;
	nPeticiones : int;
	estado : int;
	directorio : string;
};

tInfoServers : adt
{
	server : array of tServer;
	nTotal: int;
};
##############################################

#Variables globales
argv0: string;
##############################################

#Excepcion de mal uso
usage()
{
	fprint(sys->fildes(2), "uso: %s <directorio_1> <direccion_server_directorio_1> [... <directorio_n> <direccion_server_directorio_n>] <direccion_separador>\n", argv0);
	raise "fail:usage";
}
##############################################


##############################################
getInfoEstado (estado : int) : string
# - Devuelve un string informativo del estado de conexion
##############################################
{

	case estado {
		0 => return "no se ha intentado conectar";
		1 => return "servidor conectado";
		-1 => return "fue imposible conectar con el servidor";
		* => return "Código inválido!";
	}
}
###############################################


################################################
pintartInfoServers(listaServers : ref tInfoServers)
# - Muestra por pantalla el contenido de la lista de servidores
################################################
{
	i: int;

	sys->print("Nº total de servers : %s \n", string (listaServers.nTotal - 1));

	for(i=0; i<listaServers.nTotal; i++)
	{
		if (i==0)
		{
			sys->print("--------------- Broadcast ------------------------\n");
			sys->print("Nº peticiones de broadcast : %s \n", 
				  string listaServers.server[i].nPeticiones);
			sys->print("------------------------------------------------\n");
		}else{
			sys->print("--------------- Servidor  %s -----------------------\n", string (i));
			sys->print("Direccion server : %s \n", listaServers.server[i].direccion);
			sys->print("Directorio que siver %s \n", listaServers.server[i].directorio);
			sys->print("Nº peticiones realizadas a este server : %s \n", 
				  string listaServers.server[i].nPeticiones);
			sys->print("Estado de conexion : %s \n", getInfoEstado(listaServers.server[i].estado));
			sys->print("------------------------------------------------\n");
		}
	}
}
###############################################



###############################################
inicializartInfoServers(listaServers : ref tInfoServers, nTotal : int)
# - Inicializa una estructura de tInfoServers
###############################################
{
	i: int;

	#Guardamos el nTotal (nº de argv -1)
	#Le sumamos 1 mas!. La pos 0 la reservamos para broadcast
	listaServers.nTotal= nTotal + 1;

	#Instanciamos el array...¡aqui se le da tamaño!
	listaServers.server = array [listaServers.nTotal] of tServer;
	
	
	#y damos nulos a los nodos de la estructura ( a todo menos a los FD)
	for(i=0; i<listaServers.nTotal; i++)
	{
		listaServers.server[i].direccion = nil;
		listaServers.server[i].directorio = nil;
		listaServers.server[i].nPeticiones = 0;
		listaServers.server[i].estado = 0;
	}
}
###############################################




###############################################
conectarConServers(listaServers : ref tInfoServers)
# - Intenta conectar con todos los servidores almacenados
###############################################
{
	i : int;

	#Empezamos a partir del 1, 0 para broadcast!
	for (i=1; i<listaServers.nTotal; i++)
	{
		sys->print("conectando con %s ...", listaServers.server[i].direccion);

		#Llamamos a la funcion connect con nuestra dir
		listaServers.server[i].conexion = connect(listaServers.server[i].direccion);

		#Si no fue posible, lo indicamos en su estado
		if (listaServers.server[i].conexion == nil )
		{
			listaServers.server[i].estado = -1;
			sys->print("¡falló la conexion! \n");
		}else{
			listaServers.server[i].estado = 1;
			sys->print("ok! \n");
		}
	}
}
#################################################



###############################################
algunServer ( listaServers: ref tInfoServers) : int
# - Mira si queda algun servidor vivo
###############################################
{
	i:= 1;
	encontrado := -1;

	while( (i<listaServers.nTotal) && (encontrado==-1) )
	{
		if (listaServers.server[i].estado==1)
		{
			encontrado = 1;
		}
		i++;
	}
	
	return encontrado;
}
#################################################



##############################################################
 directorioServido(listaServers : ref tInfoServers, directorioBuscado : string) : int
# - Busca si alguno de los servers sirve ese directorio
# - Controla que no escojamos los que ya sabemos que han caído
# - Reservaremos val -1 para no encontrado/no activo. 0 para broadcast
##############################################################
{
	hayAlguno, encontrado, i, posServer: int;

	hayAlguno = algunServer(listaServers);
	posServer = -1;

	if ( (hayAlguno!=-1)||(listaServers.nTotal==0) )
	{
		encontrado=-1;
		i = 1;
		while ( (i<listaServers.nTotal) && (encontrado <0) )
		{
			sys->print("Comparando\n>>>%s\n>>>%s\n", listaServers.server[i].directorio, directorioBuscado);
			if ((listaServers.server[i].directorio==directorioBuscado) && (listaServers.server[i].estado>0) )
			{
				sys->print("hay uno que lo sirve!\n");
				encontrado =1;
				posServer = i;
			}
			i++;
		}
		
			

	}else{
		sys->print("¡¡¡¡¡¡ No hay ningún servidor activo !!!!!\n");
		posServer = -1;
	}
	
	return posServer;
}

################################################################



###########################################################
acceptcli(c: Sys->Connection): ref FD
# - Realiza el proceso de conexion de un cliente, a partir de su direccion
###########################################################
{
	#(e, c) := announce(addr);
	#if (e < 0)
		#raise sprint("fail: can't announce: %r");
	(es, ccon) := listen(c);
	if (es < 0)
		raise sprint("fail: can't listen: %r");
	ccon.dfd = open(ccon.dir + "/data", ORDWR);
	if (ccon.dfd == nil)
		raise sprint("fail: can't open connection: %r");
	return ccon.dfd;
}
###########################################################



###########################################################
kill(pid : int)
# - Mata un proceso, dado su pid
###########################################################
{
	f := open("/prog/" + string pid + "/ctl", OWRITE);
	if (f == nil)
		return;
	write(f, array of byte "kill", 4);
}




###################################################################
reqdbBroadcast(buffer_cliente: array of byte, listaServers: ref tInfoServers, dbg: ref FD)
# - Muestra el contenido de la peticion del cliente, y se la envia a todos los paginas
# - Le pasamos el buffer de peticion directamente
# - Tiene acceso a la ED, asi q si detecta algun server caido; lo anotará
###################################################################
{
	i: int;

	#(buf, e) := readmsg(in, 10*1024);

	#Controlamos q se leyera correctamente
	#if (e != nil){
		#fprint(dbg, "req: readmsg: %s", e);
		#exit;
	#}

	(m, es) := Reqmsg.unpack(buffer_cliente);

	#Controlamos que se desempaquete correctamente
	if (es != nil)
	{
		fprint(dbg, "req: readmsg: %s", es);
		exit;
	}

	

	for(i=1; i<listaServers.nTotal; i++)
	{
		fprint(dbg, "cli→srv: %s\n", m.text());
		sys->print("Enviando peticion a %s <pos %s> \n", 
				   listaServers.server[i].direccion, string i);
		#Si todo fue correcto, se lo enviamos (un write al fd_de la conex del server)
		wl := write(listaServers.server[i].conexion, buffer_cliente, len buffer_cliente);
		
		if (wl != len buffer_cliente)
		{
			fprint(dbg, "req: can't write msg: %r\n");
			exit;
		}
	}
}
###################################################################




##############################################################
reqdb(buffer_cliente: array of byte, out: ref FD, dbg: ref FD) : int
# - Muestra el contenido de la peticion del cliente, y se la envia a un paginas
# - Le pasamos el array de bytes. Leemos la peticion en el PP!
# - Devuelve -1 si hubo algún error, 1 en caso contrario
##############################################################
{
	#(buffer_cliente, e) := readmsg(in, 10*1024);

	#Controlamos q se leyera correctamente
#	if (e != nil){
#		fprint(dbg, "req: readmsg: %s", e);
#		exit;
#	}

	(m, es) := Reqmsg.unpack(buffer_cliente);

	#Controlamos que se desempaquetara correctamente
	if (es != nil)
	{
		fprint(dbg, "req: readmsg: %s", es);
		return -1;
		exit;
	}

	
	fprint(dbg, "cli→srv: %s\n", m.text());
	#Si todo fue correcto, se lo enviamos (un write al fd_de la conex del server)
	wl := write(out, buffer_cliente, len buffer_cliente);
	if (wl != len buffer_cliente)
	{
		fprint(dbg, "req: can't write msg: %r\n");
		return -1;
		exit;
	}else{
		return 1;
	}
}
##############################################################



##############################################################
repdbBroadcast(listaServers: ref tInfoServers, out: ref FD, dbg: ref FD)
#Recoge las peticiones de todos los paginas, y envia al cliente solo una de ellas
##############################################################
{
	i: int;
	esPrimeraVez : int;

	esPrimeraVez = 1;

	#Recogemos tantas peticiones como nServers
	for (i=1; i<listaServers.nTotal; i++)
	{
		(buf, e) := readmsg(listaServers.server[i].conexion, 10*1024);

		#Controlamos q se leyera correctamente
		if (e != nil){
			fprint(dbg, "rep: readmsg: %s", e);
			exit;
		}

		(m, es) := Repmsg.unpack(buf);

		#Controlamos que se desempaquetara correctamente
		if (es != nil){
			fprint(dbg, "rep: readmsg: %s", es);
			exit;
		}


		fprint(dbg, "srv→cli: %s\n", m.text());
		
		#Y lo enviamos solo una vez al cliente (si no, se bloqueará)
		if (esPrimeraVez==1)
		{
			esPrimeraVez = 0;
			#Si todo fue correcto, se lo enviamos (un write al fd_de la conex del cliente)
			wl := write(out, buf, len buf);
			if (wl != len buf)
			{
				fprint(dbg, "rep: can't write msg: %r\n");
				exit;
			}
		}
	}
}
##############################################################



##############################################################
repdb(in: ref FD, out: ref FD, dbg: ref FD) : int
# - Muestra el contenido de la respuesta del paginas, y se la envia al cliente
# - Seguimos pasandole la conexion!
##############################################################
{
	(buf, e) := readmsg(in, 10*1024);

	#Controlamos q se leyera correctamente
	if (e != nil)
	{
		fprint(dbg, "rep: readmsg: %s", e);
		return -1;
		exit;
	}

	(m, es) := Repmsg.unpack(buf);

	#Controlamos que se desempaquetara correctamente
	if (es != nil)
	{
		fprint(dbg, "rep: readmsg: %s", es);
		return -1;
		exit;
	}


	fprint(dbg, "srv→cli: %s\n", m.text());

	#Si todo fue correcto, se lo enviamos (un write al fd_de la conex del cliente)
	wl := write(out, buf, len buf);
	if (wl != len buf)
	{
		fprint(dbg, "rep: can't write msg: %r\n");
		return -1;
		exit;
	}else{
		return 1;
	}
}
##############################################################



###############################################
netmkaddr(addr, net, svc: string): string
# - Prepara el formato de la conexion
###############################################
{
	# borrowed from /appl/cmd/mount.b too.
	if(net == nil)
		net = "net";
	(n, l) := sys->tokenize(addr, "!");
	if(n <= 1){
		if(svc== nil)
			return sys->sprint("%s!%s", net, addr);
		return sys->sprint("%s!%s!%s", net, addr, svc);
	}
	if(svc == nil || n > 2)
		return addr;
	return sys->sprint("%s!%s", addr, svc);
}
###############################################


########################################################################
connect(dest: string): ref FD
# - Realiza la conexion con un paginas, dada su direccion en formato: tcp!<maquina>!<puerto>
# - Devuelve el descriptor de la conexion
########################################################################
{
	# Code borrowed from /appl/cmd/mount.b

	#Devuelve una lista de strings, separando por el delimitador (en este caso "!")
	(n, nil) := tokenize(dest, "!");
	
	if(n == 1){
		fd := open(dest, ORDWR);
		if(fd != nil)
			return fd;
		if(dest[0] == '/') 
			return nil;
	}
	(ok, c) := dial(netmkaddr(dest, "tcp", nil), nil);
	if(ok < 0)
		return nil;
	return c.dfd;
}

########################################################################
tratar_cliente(conexCliente : ref FD, listaServers : ref tInfoServers)
# - Se encarga de gestionar las peticiones de un cliente
# - Manipula el contenido de la peticion
# - Realiza el envio y recepcion
########################################################################
{
	#Variables de gestion de conexión
	peticionCliente : array of byte;
	es: string;
	e1, e2 :int;
	pos : int;
	salir : int;

	#Variables para recoger el contenido desempaquetado
	peticionUnpack : ref Reqmsg;
	rutaCompleta, directorio, archivo : string;
	
	for(;;)
	{
		salir = 0;	
		pintartInfoServers(listaServers);

		#Leemos la peticion del cliente
		(peticionCliente, es) = readmsg(conexCliente, 10*1024);

		#Controlamos q se leyera correctamente
		if (es != nil)
		{
			#sys->print("error al leer la petición del cliente %s \n", es);
			#exit;
			sys->print("Fin de conexion con cliente\n");
			sys->print("------------------------------------------\n");
			#end of connection
			return;
		}else{
			#Desempaquetamos, ya que necesitamos saber el nombre
			(peticionUnpack, es) = Reqmsg.unpack(peticionCliente);
			if (es != nil)
			{
				sys->print("Error al desempaquetar! \n");
			}else{
				#Tenemos que tomar la estructura, en funcion de su tipo
				pick r := peticionUnpack {
					Exit =>
						#respuesta = ref Repmsg.Exit();
						sys -> print("Recibida peticion de finalización\n");
						sys-> print("Fin de conexion con cliente\n");
						sys-> print("------------------------------------------\n");
						salir=1;
	
					Get  =>
						#Descomponemos la ruta en directorio-nombre
						rutaCompleta = r.name;
						#La forma de recortar actual es:
						# /tmp1/tmp2/arch3
						# id: tmp1
						#envio: /tmp2/arch3
						(directorio,archivo)=str->splitl(rutaCompleta[1:], "/");
						sys->print(">>>>%s>>>>%s>>>>%s", rutaCompleta, directorio, archivo);
						#Y modificamos la peticion. [Añadimos ./ en version casa]
						#r.name = "./" + archivo;
						r.name = archivo;
						peticionUnpack = r;
		
					Put =>
						#Descomponemos la ruta en directorio-nombre
						rutaCompleta = r.name;
						(directorio,archivo)=str->splitl(rutaCompleta[1:], "/");
						
						#Y modificamos la peticion
						#r.name = "./" + archivo;
						r.name= archivo;
						peticionUnpack = r;
						sys->print("peticion no tratada\n!");
				}
				#Volvemos a empaquetar con los cambios
				peticionCliente = peticionUnpack.pack();
				
				#Buscaremos si servimos el directorio
				pos = directorioServido(listaServers,directorio);
				if (pos>0) 
				{
					sys->print("encontrado en la posicion %s\n", string pos);

					#Ahora, se lo enviaremos al correspondiente (o a todos si es 0)
					if (pos==0)
					{	
						#Aumentamos cuenta de broadcast
						listaServers.server[pos].nPeticiones++;
				
						#En la propia llamada se detectan servidores caidos
						#Llamamos a la funcion que hemos creado, que se lo envia a todos
						#Le pasamos la peticion, ya que solo se necesita leer una vez
						reqdbBroadcast(peticionCliente,listaServers,fildes(2));

						#Lo recogemos con la funcion que hemos creado, que recoge todo; pero
						#envia solo el contenido de uno de ellos
						repdbBroadcast(listaServers,conexCliente,fildes(2));
					}else{
			
						sys->print("Enviando peticion a %s <pos %s> \n", 
						   listaServers.server[pos].direccion, string pos);
						listaServers.server[pos].nPeticiones++;

						e1 =reqdb(peticionCliente, listaServers.server[pos].conexion, fildes(2));
						if (e1<0)
						{
							sys->print("Error en el reqdb. Marcamos server caido\n !");
							listaServers.server[pos].estado = -1;	
						}else{

							e2 = repdb(listaServers.server[pos].conexion, conexCliente, fildes(2));			
							if (e2<0)
							{
								sys->print("Error en el repdb. Marcamos server caido \n");
								listaServers.server[pos].estado = -1;	
							}#e2<0
						}#e1<0
					}#pos==0

				}else{
					sys->print("¡no encontrado! valor de pos...%s\n", string pos);				
				}#pos>0
			}#Control_unpack
		}#Control_lectura
        
		 if(salir)
		{
			sys->print("Se desconecto un cliente\n");
			exit;
		}


	}#Bucle infinito
}
########################################################################

################################################################
#Programa principal
################################################################
init(nil: ref Draw->Context, argv: list of string)
{
	#Declaracion de variables
	nServers : int;
	i : int;
	direccSeparador : string;
	conexCliente : ref FD;
	ultCar, primCar : string;
	#Declaramos e instanciamos la lista de servers
	listaServers := ref tInfoServers;


	#Eliminamos el nombre del programa de la lista de argumentos
	argv0 = hd argv;
	argv  = tl argv;

	#Carga e inicializacion de modulos
	sys = load Sys Sys->PATH;
	wp = load Wp Wp->PATH;
	wp->setup();
	str = load String String->PATH;

	#Controlamos que al menos nos den el direct. de un server y su direccion + la del repartidor
	#y que sea impar (pares de directorio-server + dir de servidor)
	if ( (len argv < 3) || ( (len argv) % 2==0) )
	{
		usage();
	}

	#Calculamos el nº total de servers con el que nos comunicaremos (div ent de param)
	#Se le suma uno para broadcast en la inicializacion!
	nServers =  (len(argv))/2;
	sys->print("hay q almacenar %s servers \n", string nServers);

	#Inicializamos la estructura de servers
	inicializartInfoServers(listaServers,nServers);
	
	#Recogida de parámetros
	########################################################
	#Y guardamos los pares directorio/server
	#Tenemos que coger todas menos la ultima. Almacenamos a partir de 1!
	i=1;
	#La identificacion del server se hace mediante el directorio sin barras
	# ej.: /tmp/tmp2/f1 ---> id=tmp
	while ( (len argv)>1 )
	{
		listaServers.server[i].directorio = hd argv;
		#Eliminamos las barras tanto en ppo, como en final
		primCar = listaServers.server[i].directorio[0:1];
		if (primCar=="/")
		{
			listaServers.server[i].directorio = listaServers.server[i].directorio[1:(len listaServers.server[i].directorio)];
		}
		ultCar = listaServers.server[i].directorio[(len (listaServers.server[i].directorio)-1):len (listaServers.server[i].directorio)];
	
		if (ultCar == "/")
		{
			listaServers.server[i].directorio= listaServers.server[i].directorio[:(len (listaServers.server[i].directorio)-1)];
		}

		sys->print("cadena recortada <<%s>>\n", listaServers.server[i].directorio);
		argv= tl argv;
		listaServers.server[i].direccion = hd argv;
		argv = tl argv;
		i++;
	}
	pintartInfoServers(listaServers);

	#y guardamos nuestra direccion de separador
	direccSeparador  = hd argv;
	########################################################

	#Hacemos la conexion con los servers
	conectarConServers(listaServers);
	pintartInfoServers(listaServers);
	
	#Preparamos nuestro descriptor de conexion
	(e, c) := announce(direccSeparador);
	if (e < 0)
	{
		raise sprint("error al realizar el announce :  %r");
	}

	for(;;)
	{
		sys->print("Esperando clientes...\n");
		#Crearemos un socket por cliente 
		conexCliente = acceptcli(c);
		#Y le damos un thread a cada cliente
		spawn tratar_cliente(conexCliente, listaServers);
	}
}


