#########################################################
# PRACTICA ASO : Servidor web distribuido
# ---------------------------------------
# Fase actual : 5 (repartidor de carga, con errores de servers)
# Fichero : repartidor.b
# Descripcion : Reparte la carga de peticiones entre diversos servidores
#    			de paginas.  
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
	announce, listen, OWRITE, write : import sys;
include "draw.m";

include "wp.m";
	wp : Wp;
	readmsg, Reqmsg, Repmsg: import wp;

#Constantes


#Definicion de tipos para almacenar info de servers
tServer : adt
{
	direccionServer : string;
	conexionServer : ref FD;
	nPeticiones : int;
	estado : int;
};

tInfoServers : adt
{
	server : array of tServer;
	nTotal: int;
	sig : int;
};
##############################################

#Variables globales
argv0: string;
##############################################

#Excepcion de mal uso
usage()
{
	fprint(sys->fildes(2), "uso: %s <direccion_paginas1> [...direccion_paginasN] <direccion_repartidor>\n", argv0);
	raise "fail:usage";
}
##############################################


#Devuelve un string informativo del estado de conexion
##############################################
getInfoEstado (estado : int) : string
{

	case estado {
		0 => return "no se ha intentado conectar";
		1 => return "servidor conectado";
		-1 => return "fue imposible conectar con el servidor";
		* => return "Código inválido!";
	}
}

#Muestra por pantalla el contenido de la lista de servidores
##############################################
pintartInfoServers(listaServers : ref tInfoServers)
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
			sys->print("--------------- Paginas %s ----------------------\n", string (i));
			sys->print("Direccion server : %s \n", listaServers.server[i].direccionServer);
			sys->print("Nº peticiones realizadas a este server : %s \n", 
				  string listaServers.server[i].nPeticiones);
			sys->print("Estado de conexion : %s \n", getInfoEstado(listaServers.server[i].estado));
			sys->print("------------------------------------------------\n");
		}
	}
}
###############################################


#Inicializa una estructura de tInfoServers
###############################################
inicializartInfoServers(listaServers : ref tInfoServers, nTotal : int)
{
	i: int;

	#Guardamos el nTotal (nº de argv -1)
	#Le sumamos 1 mas!. La pos 0 indicará que se lo enviaremos a todos
	listaServers.nTotal= nTotal + 1;

	#Instanciamos el array...¡aqui se le da tamaño!
	listaServers.server = array [listaServers.nTotal] of tServer;
	
	#Empezamos en 0. Al llamar a poltica ya correremos una posicion
	listaServers.sig=0;
	
	#y damos nulos a los nodos de la estructura ( a todo menos a los FD)
	for(i=0; i<listaServers.nTotal; i++)
	{
		listaServers.server[i].direccionServer = nil;
		listaServers.server[i].nPeticiones = 0;
		listaServers.server[i].estado = 0;
	}
}
###############################################



#Intenta conectar con todos los servidores almacenados
###############################################
conectarConServers(listaServers : ref tInfoServers)
{
	i : int;

	#Empezamos a partir del 1, 0 para broadcast!
	for (i=1; i<listaServers.nTotal; i++)
	{
		sys->print("conectando con %s ...", listaServers.server[i].direccionServer);

		#Llamamos a la funcion connect con nuestra dir
		listaServers.server[i].conexionServer = connect(listaServers.server[i].direccionServer);

		#Si no fue posible, lo indicamos en su estado
		if (listaServers.server[i].conexionServer == nil )
		{
			listaServers.server[i].estado = -1;
			sys->print("¡falló la conexion! \n");
		}else{
			listaServers.server[i].estado = 1;
			sys->print("ok! \n");
		}
	}
}


#Mira si queda algun servidor vivo
###############################################
algunServer ( listaServers: ref tInfoServers) : int
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

#Actualiza el nº de server que atenderá la peticion (usamos round-robin)
#Controla que no escojamos los que ya sabemos que han caído
########################################################
politica (listaServers : ref tInfoServers)
{
	hayAlguno, encontrado: int;

	hayAlguno = algunServer(listaServers);

	if ( (hayAlguno!=-1)||(listaServers.nTotal==0) )
	{
		encontrado=-1;
		#Nos hemos asegurado de q al menos hay uno previamente, asi q iteramos hasta encontrarlo
		do
		{
			#Si es el último, ¡ojo a la comp!
			if (listaServers.sig == (listaServers.nTotal -1))
			{
				listaServers.sig = 1;
			}else{
				listaServers.sig++;
			}

			#Y ahora, comprobamos q sea valido

			if (listaServers.server[listaServers.sig].estado==1)
			{
				sys->print("Siguiente server en atender---> %s <<pos %s>>\n", 
						   listaServers.server[listaServers.sig].direccionServer, 
						   string listaServers.sig);
				encontrado =1;
			}else{
				sys->print("Servidor descartado, parece caido---> %s \n",
						   listaServers.server[listaServers.sig].direccionServer);
			}
		}while(encontrado<0);
			

	}else{
		sys->print("¡¡¡¡¡¡ No hay ningún servidor activo !!!!!\n");
		listaServers.sig=-1;
	}
}

###############################################


#Programa principal
###############################################
init(nil: ref Draw->Context, argv: list of string)
{
	#Declaracion de variables
	nServers : int;
	i : int;
	direccRepartidor : string;
	conexCliente : ref FD;
	#nPeticionesTotal : int;
	#serverElegido : int;
	#Declaramos e instanciamos la lista de servers
	listaServers := ref tInfoServers;
	
	#Variables de gestion de conexión
	peticionCliente : array of byte;
	es: string;
	e1, e2 :int;


	#Eliminamos el nombre del programa de la lista de argumentos
	argv0 = hd argv;
	argv  = tl argv;

	#Carga e inicializacion de modulos
	sys = load Sys Sys->PATH;
	wp = load Wp Wp->PATH;
	wp->setup();

	#Controlamos que al menos nos den la dir de un paginas, y la del repartidor
	if (len argv < 2)
	{
		usage();
	}

	#Calculamos el nº total de servers con el que nos comunicaremos
	nServers =  (len(argv))-1;

	#Inicializamos la estructura de servers
	inicializartInfoServers(listaServers,nServers);
	
	#Recogida de parámetros
	########################################################
	#Y guardamos todas las direcciones que nos han pasado por shell
	#Tenemos que coger todas menos la ultima. Almacenamos a partir de 1!
	i=1;
	while ( (len argv)>1 )
	{
		listaServers.server[i].direccionServer = hd argv;
		argv= tl argv;
		i++;
	}
	#pintartInfoServers(listaServers);

	#y guardamos nuestra direccion de repartidor
	direccRepartidor  = hd argv;
	########################################################

	#Hacemos la conexion con los servers
	conectarConServers(listaServers);
	pintartInfoServers(listaServers);
	
	#aceptamos las conexiones de los clientes
	conexCliente = acceptcli(direccRepartidor);
	#nPeticionesTotal = 0;

	for(;;)
	{
		
		#pintartInfoServers(listaServers);

		#Leemos la peticion del cliente, para no tener q volver a leer si falla el server.
		(peticionCliente, es) = readmsg(conexCliente, 10*1024);

		#Controlamos q se leyera correctamente
		if (es != nil)
		{
			sys->print("error al leer la petición del cliente %s \n", es);
			exit;
		}else{

			
			#Llamaremos para escoger un server
			politica(listaServers);

		
			#Ahora, se lo enviaremos al correspondiente (o a todos si es 0)
			if (listaServers.sig==0)
			{	
				#Aumentamos cuenta de broadcast
				listaServers.server[listaServers.sig].nPeticiones++;
				
				#En la propia llamada se detectan servidores caidos
				#Llamamos a la funcion que hemos creado, que se lo envia a todos
				#Le pasamos la peticion, ya que solo se necesita leer una vez
				reqdbBroadcast(peticionCliente,listaServers,fildes(2));

				#Lo recogemos con la funcion que hemos creado, que recoge todo; pero
				#envia solo el contenido de uno de ellos
				repdbBroadcast(listaServers,conexCliente,fildes(2));

			}else{
			
				#Con este bucle, detectamos nuevos servidores caidos
				do
				{
					sys->print("Enviando peticion a %s <pos %s> \n", 
						   listaServers.server[listaServers.sig].direccionServer, string listaServers.sig);
					listaServers.server[listaServers.sig].nPeticiones++;

					e1 =reqdb(peticionCliente, listaServers.server[listaServers.sig].conexionServer, fildes(2));
					if (e1<0)
					{
						sys->print("Error en el reqdb\n !");
						#listaServers.server[serverElegido].estado = -1;
					}else{

						e2 = repdb(listaServers.server[listaServers.sig].conexionServer, conexCliente, fildes(2));		
						if (e2<0)
						{
							sys->print("Error en el repdb \n");
							#listaServers.server[serverElegido].estado = -1;
						}
					}


					#Si hubo algun error, tenemos q volver a escoger server
					if ((e1<0)||(e2<0))
					{
						listaServers.server[listaServers.sig].estado = -1;	
						sys->print("¡El servidor %s parece caído!\n", string listaServers.sig);
						politica(listaServers);
					}
				
				}while( ( (e1<0) || (e2<0) ) && (listaServers.sig!=-1) );

				#Si hay ningun server, forzamos la salida
				if (listaServers.sig==-1)
				{
					sys->print("Abortada ejecución de repartidor, por falta de servidores activos...bye!\n");
					exit;
				}
			}
		}
	}
}


#Acepta conexiones de un cliente, en la direccion que le indicamos por shell
###########################################################
acceptcli(addr: string): ref FD
{
	(e, c) := announce(addr);
	if (e < 0)
		raise sprint("fail: can't announce: %r");
	(es, ccon) := listen(c);
	if (es < 0)
		raise sprint("fail: can't listen: %r");
	ccon.dfd = open(ccon.dir + "/data", ORDWR);
	if (ccon.dfd == nil)
		raise sprint("fail: can't open connection: %r");
	return ccon.dfd;
}

#Mata un proceso, dado su pid
###########################################################
kill(pid : int)
{
	f := open("/prog/" + string pid + "/ctl", OWRITE);
	if (f == nil)
		return;
	write(f, array of byte "kill", 4);
}

#Muestra el contenido de la peticion del cliente, y se la envia a todos los paginas
# - Le pasamos el buffer de peticion directamente
# - Tiene acceso a la ED, asi q si detecta algun server caido; lo anotará
##############################################################
reqdbBroadcast(buffer_cliente: array of byte, listaServers: ref tInfoServers, dbg: ref FD)
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
				   listaServers.server[i].direccionServer, string i);
		#Si todo fue correcto, se lo enviamos (un write al fd_de la conex del server)
		wl := write(listaServers.server[i].conexionServer, buffer_cliente, len buffer_cliente);
		
		if (wl != len buffer_cliente)
		{
			fprint(dbg, "req: can't write msg: %r\n");
			exit;
		}
	}
}


#Muestra el contenido de la peticion del cliente, y se la envia a un paginas
# - Le pasamos el array de bytes. Leemos la peticion en el PP!
# - Devuelve -1 si hubo algún error, 1 en caso contrario
##############################################################
reqdb(buffer_cliente: array of byte, out: ref FD, dbg: ref FD) : int
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

#Recoge las peticiones de todos los paginas, y envia al cliente solo una de ellas
##############################################################
repdbBroadcast(listaServers: ref tInfoServers, out: ref FD, dbg: ref FD)
{
	i: int;
	esPrimeraVez : int;

	esPrimeraVez = 1;

	#Recogemos tantas peticiones como nServers
	for (i=1; i<listaServers.nTotal; i++)
	{
		(buf, e) := readmsg(listaServers.server[i].conexionServer, 10*1024);

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
 





#Muestra el contenido de la respuesta del paginas, y se la envia al cliente
#Seguimos pasandole la conexion!
##############################################################
repdb(in: ref FD, out: ref FD, dbg: ref FD) : int
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

#Prepara el formato de la conexion
########################################################################
netmkaddr(addr, net, svc: string): string
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

#Realiza la conexion con un paginas, dada su direccion en formato: tcp!<maquina>!<puerto>
#Devuelve edl descriptor de la conexion
########################################################################
connect(dest: string): ref FD
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
