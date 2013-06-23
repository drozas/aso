#########################################################
# PRACTICA ASO : Servidor web distribuido
# ---------------------------------------
# Fase actual : 4 (repartidor de carga)
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

	sys->print("Nº total de servers : %s \n", string (listaServers.nTotal));

	for(i=0; i<listaServers.nTotal; i++)
	{
		if (i==0)
		{
			sys->print("--------------- Paginas %s : Broadcast ----------\n", 
					  string (i));
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


#Devuelve el num del servidor al que haremos la peticion
###############################################
politica (nTotalPeticiones : int , nTotalServers : int) : int
{
	#Devolvemos el modulo
	sys->print("%s mod %s\n", string nTotalPeticiones, string nTotalServers);
	return (nTotalPeticiones % nTotalServers);
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
	nPeticionesTotal : int;
	serverElegido : int;
	#Declaramos e instanciamos la lista de servers
	listaServers := ref tInfoServers;
	


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
	#pintartInfoServers(listaServers);
	
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
	nPeticionesTotal = 0;
	for(;;)
	{
		pintartInfoServers(listaServers);
		#Aumentamos el nº total de peticiones, y elegimos uno
		nPeticionesTotal++;
		serverElegido = politica(nPeticionesTotal, listaServers.nTotal);
		listaServers.server[serverElegido].nPeticiones++;

		
		#Ahora, se lo enviaremos al correspondiente (o a todos si es 0)
		if (serverElegido==0)
		{	
			sys->print("---->Petición de Broadcast \n");
			for(i=1; i<listaServers.nTotal; i++)
			{
				sys->print("Enviando peticion a %s <pos %s> \n", 
						   listaServers.server[i].direccionServer, string i);
				reqdb(conexCliente, listaServers.server[i].conexionServer, fildes(2));
				repdb(listaServers.server[i].conexionServer, conexCliente, fildes(2));
			}

		}else{
				sys->print("Enviando peticion a %s <pos %s> \n", 
						   listaServers.server[serverElegido].direccionServer, string serverElegido);
				reqdb(conexCliente, listaServers.server[serverElegido].conexionServer, fildes(2));
				repdb(listaServers.server[serverElegido].conexionServer, conexCliente, fildes(2));
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

#Muestra el contenido de la peticion del cliente, y se la envia al paginas
#############################################################
reqdb(in: ref FD, out: ref FD, dbg: ref FD)
{
	(buf, e) := readmsg(in, 10*1024);

	#Controlamos q se leyera correctamente
	if (e != nil){
		fprint(dbg, "req: readmsg: %s", e);
		exit;
	}

	(m, es) := Reqmsg.unpack(buf);

	#Controlamos que se desempaquetara correctamente
	if (es != nil)
	{
		fprint(dbg, "req: readmsg: %s", es);
		exit;
	}

	
	fprint(dbg, "cli→srv: %s\n", m.text());
	#Si todo fue correcto, se lo enviamos (un write al fd_de la conex del server)
	wl := write(out, buf, len buf);
	if (wl != len buf){
		fprint(dbg, "req: can't write msg: %r\n");
		exit;
	}
}

#Muestra el contenido de la respuesta del paginas, y se la envia al cliente
##############################################################
repdb(in: ref FD, out: ref FD, dbg: ref FD)
{
	(buf, e) := readmsg(in, 10*1024);

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

	#Si todo fue correcto, se lo enviamos (un write al fd_de la conex del cliente)
	wl := write(out, buf, len buf);
	if (wl != len buf){
		fprint(dbg, "rep: can't write msg: %r\n");
		exit;
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
