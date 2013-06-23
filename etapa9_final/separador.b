#########################################################
# PRACTICA ASO : Servidor web distribuido
# ---------------------------------------
# Fase actual : 9 ("Tolerando caidas del repartidor"). Version final.
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
	announce, listen, OWRITE, write, sleep, NEWPGRP: import sys;
include "draw.m";

include "string.m";
	str : String;

include "wp.m";
	wp : Wp;
	readmsg, Reqmsg, Repmsg: import wp;

#Constantes
TAM_BLOQUE: con 1024;

#Definicion de tipos para almacenar info de servers
#El identificador se guarda sin "/", Ej.:
						# Para -> "/tmp1/tmp2/arch3"
						# identificador: tmp1
						# ruta enviada al paginas: /tmp2/arch3
tServer : adt
{
	direccion : string;
	conexion : ref FD;
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

#Excepciones
usage()
{
	fprint(sys->fildes(2), "uso: %s <directorio_1> <direccion_server_directorio_1> [... <directorio_n> <direccion_server_directorio_n>] <direccion_separador>\n", argv0);
	raise "fail:usage";
}
##############################################

################################################
pintartInfoServers(listaServers : ref tInfoServers)
# - Muestra por pantalla el contenido de la lista de servidores
################################################
{
	i: int;

	sys->print("Nº total de servers : %s \n", string (listaServers.nTotal - 1));

	for(i=1; i<listaServers.nTotal; i++)
	{
		sys->print("--------------- Servidor  %s -----------------------\n", string (i));
		sys->print("Direccion server : %s \n", listaServers.server[i].direccion);
		sys->print("Directorio servido: %s \n", listaServers.server[i].directorio);
		sys->print("------------------------------------------------\n");
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
	
	for(i=0; i<listaServers.nTotal; i++)
	{
		listaServers.server[i].direccion = nil;
		listaServers.server[i].directorio = nil;
	}
}
###############################################


###############################################
conectarConServers(listaServers : ref tInfoServers)
# - Intenta conectar con todos los servidores almacenados
###############################################
{
	i : int;

	#Empezamos a partir del 1, 0 reservado para broadcast
	for (i=1; i<listaServers.nTotal; i++)
	{
		sys->print("conectando con %s ...", listaServers.server[i].direccion);

		#Llamamos a la funcion connect con nuestra dir
		listaServers.server[i].conexion = connect(listaServers.server[i].direccion);

		if (listaServers.server[i].conexion == nil )
		{
			sys->print("¡falló la conexion! \n");
		}else{
			sys->print("ok! \n");
		}
	}
}
#################################################


##############################################################
 directorioServido(listaServers : ref tInfoServers, directorioBuscado : string) : int
# - Busca si alguno de los servers sirve ese directorio
# - 
##############################################################
{
	encontrado, i, posServer: int;

	posServer = -1;

	if(listaServers.nTotal!=0)
	{
		encontrado=-1;
		i = 1;
		while ( (i<listaServers.nTotal) && (encontrado <0) )
		{
			if(listaServers.server[i].directorio==directorioBuscado)
			{
				encontrado =1;
				posServer = i;
			}

			i++;
		}			

	}else{
		sys->print("separador->directorioServido: No hay ningún servidor activo!\n");
		posServer = -2;
	}
	
	return posServer;
}

################################################################


###########################################################
acceptcli(c: Sys->Connection): ref FD
# - Realiza el proceso de conexion de un cliente, a partir de su direccion
###########################################################
{
	(es, ccon) := listen(c);
	if (es < 0)
		raise sprint("separador->acceptcli : fail: can't listen: %r");
	ccon.dfd = open(ccon.dir + "/data", ORDWR);
	if (ccon.dfd == nil)
		raise sprint("separador->acceptcli : fail: can't open connection: %r");
	return ccon.dfd;
}
###########################################################


##############################################################
reqdb(buffer_cliente: array of byte, out: ref FD, dbg: ref FD) : int
# - Muestra el contenido de la peticion del cliente, y se la envia a un paginas
# - Le pasamos el array de bytes. Leemos la peticion en thread tratar_cliente
# - Devuelve -1 si hubo algún error, 1 en caso contrario
##############################################################
{

	(m, es) := Reqmsg.unpack(buffer_cliente);
	sys->print("separador->reqdb: iniciando envio simple...\n");

	#Controlamos que se desempaquetara correctamente
	if (es != nil)
	{
		fprint(dbg, "req: readmsg: %s", es);
		return -1;
	}else{

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
}
##############################################################



##############################################################
repdb(in: ref FD, out: ref FD, dbg: ref FD) : int
# - Muestra el contenido de la respuesta del paginas, y se la envia al cliente
# - Seguimos pasandole la conexion
##############################################################
{
	sys->print("separador->repdb: iniciando recepcion simple\n");
	(buf, e) := readmsg(in, 10*TAM_BLOQUE);

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
tratar_cliente(conexCliente : ref FD, listaServers : ref tInfoServers, canal_conexiones: chan of int)
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
	#salir : int;

	#Variables para recoger el contenido desempaquetado
	peticionUnpack : ref Reqmsg;
	rutaCompleta, directorio, archivo : string;
	
	for(;;)
	{
		#salir = 0;	
		pintartInfoServers(listaServers);

		#Leemos la peticion del cliente
		(peticionCliente, es) = readmsg(conexCliente, 10*1024);

		#Controlamos q se leyera correctamente
		if (es != nil)
		{
			#sys->print("separador->tratar_cliente: error al leer la petición del cliente %s \n", es);
			#exit;
			sys->print("separador->tratar_cliente:Fin de conexion con cliente\n");
			sys->print("------------------------------------------\n");
			canal_conexiones<-=-1;
			exit;
			#end of connection
			#return;?
		}else{
			#Desempaquetamos, ya que necesitamos saber el nombre
			(peticionUnpack, es) = Reqmsg.unpack(peticionCliente);
			if (es != nil)
			{
				sys->print("separador->tratar_cliente: Error al desempaquetar! \n");
			}else{
				#Tenemos que tomar la estructura, en funcion de su tipo
				pick r := peticionUnpack {
					Exit =>
						#respuesta = ref Repmsg.Exit();
						sys -> print("separador->tratar_cliente: Recibida peticion de finalización\n");
						sys-> print("separador->tratar_cliente: Fin de conexion con cliente\n");
						sys-> print("------------------------------------------\n");
						#salir=1;
						canal_conexiones<-=-1;
						exit;
	
					Get  =>
						#Descomponemos la ruta 
						rutaCompleta = r.name;
						(directorio,archivo)=str->splitl(rutaCompleta[1:], "/");
						
						#Y modificamos la peticion. 
						r.name = archivo;
						peticionUnpack = r;
		
					Put =>
						#Descomponemos la ruta 
						rutaCompleta = r.name;
						(directorio,archivo)=str->splitl(rutaCompleta[1:], "/");
						
						#Y modificamos la peticion
						r.name= archivo;
						peticionUnpack = r;
				}
				#Volvemos a empaquetar con los cambios
				peticionCliente = peticionUnpack.pack();
				
				#Buscaremos si servimos el directorio
				pos = directorioServido(listaServers,directorio);
				if (pos>0) 
				{
					#Ahora, se lo enviaremos al correspondiente (o a todos si es 0)
					if (pos==0)
					{	
						sys->print("separador->tratar_cliente: Servicio de broadcast no habilitado.\n");
					}else{
			
						sys->print("separador->tratar_cliente: Enviando peticion a %s <pos %s> \n",listaServers.server[pos].direccion, string pos);
				
						e1 =reqdb(peticionCliente, listaServers.server[pos].conexion, fildes(2));
						if (e1<0)
						{
							sys->print("separador->tratar_cliente: Error en el reqdb\n !");	
						}else{

							e2 = repdb(listaServers.server[pos].conexion, conexCliente, fildes(2));			
							if (e2<0)
							{
								sys->print("separador->tratar_cliente: Error en el repdb \n");	
							}#e2<0
						}#e1<0
					}#pos==0

				}else if (pos==-1){
					sys->print("¡no encontrado! valor de pos...%s\n", string pos);	
				}else if (pos==-2){
					sys->print("separador->tratar_cliente: no hay servers activos!\n");
					exit;			
				}#if_pos

			}#Control_unpack

		}#Control_lectura

   	}#Bucle infinito
}
########################################################################


###################################################################
checkea_conexiones (canal_entrada: chan of int, gid : int)
# - Thread que gestiona el acceso a la variable nConexiones
# - Cuando no haya conexiones, lanzar un thread q dormira 15 sg. Si llega algo nuevo
#    lo matará, y si no, este thread matará todo
###################################################################
{
	nConexiones, aux : int;

	nConexiones =0;
	gid2 : int;

	#Agrupamos todos los threads del grupo de control
	gid2 = pctl(NEWPGRP, nil);

	for(;;)
	{

		sys->print("separador->checkea_conexiones: esperando a recibir peticiones\n");
		aux = <- canal_entrada;
		sys->print("separador->checkea_conexiones: recibido el valor -> %s\n", string aux);
		nConexiones = nConexiones + aux;

		#Si es 0, lanzo el temporizador
		if(nConexiones==0) 
		{
			spawn killemall(gid);
		}else{
			#Si no, mato el temporizador
			fd:=open("/prog/" + string gid2 + "/ctl", OWRITE);
			if (fd==nil)
			{
				sys->print("separador->check_conexiones: Descriptor de grupo vacio!\n");
			}else{
				write(fd, array of byte "killgrp", 7);	
			}
			
		}
	}
}
###################################################################


###################################################################
killemall(gid: int)
# - Thread que duerme durante 15 sg, y mata a todos si no lo matan a el antes
###################################################################
{
	sys->print("separador->killemall : Me duermo 15 sg...\n");
	sleep(15000);
	sys->print("separador->killemall : voy a matar a todos...\n");

	#Abrimos el descriptor, y le escribimos señal killgrp
	fd:=open("/prog/" + string gid + "/ctl", OWRITE);
	if (fd==nil)
	{
		sys->print("separado->killemall: Descriptor de grupo vacio!\n");
	}else{
		write(fd, array of byte "killgrp", 7);	
	}
	exit;
}
###################################################################




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
	gid: int;
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

	#Calculamos el nº total de servers con el que nos comunicaremos
	#Se le suma uno para broadcast en la inicializacion
	nServers =  (len(argv))/2;

	#Inicializamos la estructura de servers
	inicializartInfoServers(listaServers,nServers);
	
	#Creamos el canal de gestion de conexiones
	canal_conexiones:= chan of int;

	#Y el grupo de threads
	gid = pctl(NEWPGRP,nil);

	#Recogida de parámetros
	########################################################
	i=1;
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

		argv= tl argv;
		listaServers.server[i].direccion = hd argv;
		argv = tl argv;
		i++;
	}

	#y guardamos nuestra direccion de separador
	direccSeparador  = hd argv;
	########################################################

	#Creamos el thread que gestiona las conexiones
	spawn checkea_conexiones(canal_conexiones, gid);

	
	#Hacemos la conexion con los servers
	conectarConServers(listaServers);
	
	#Preparamos nuestro descriptor de conexion
	(e, c) := announce(direccSeparador);
	if (e < 0)
	{
		raise sprint("separador->init : error al realizar el announce :  %r");
	}

	sys->print("separador->init : separador arrancado correctamente en %s \n", direccSeparador);
	sys->print("separador->init : las peticiones se separaran entre...\n");
	pintartInfoServers(listaServers);

	for(;;)
	{
		sys->print("separador->init : Esperando clientes...\n");
		#Crearemos un socket por cliente 
		conexCliente = acceptcli(c);
		#Le enviamos un 1 al thread de gestion de conexiones
		canal_conexiones<-=1;
		#Y creamos un thread para cada cliente
		spawn tratar_cliente(conexCliente, listaServers,canal_conexiones);
	}
}


