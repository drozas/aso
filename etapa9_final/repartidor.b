#########################################################
# PRACTICA ASO : Servidor web distribuido
# ---------------------------------------
# Fase actual : 9 ("Tolerando caidas del repartidor"). Version final.
# Fichero : repartidor.b
# Descripcion : Reparte la carga de peticiones entre diversos servidores
#    			de paginas segun round robin.
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
	announce, listen, OWRITE, write, sleep, NEWPGRP : import sys;
include "draw.m";

include "wp.m";
	wp : Wp;
	readmsg, Reqmsg, Repmsg: import wp;

#Constantes
TAM_BLOQUE: con 1024;

#Definicion de tipos para almacenar info de servers
tServer : adt
{
	direccionServer : string;
	conexionServer : ref FD;
};

tInfoServers : adt
{
	server : array of tServer;
	nTotal: int;
	sig : int;
};

#Tipo para solicitud a thread_politica
tPeticionPolitica : adt
{
	canal_respuesta: chan of string;
};
##############################################

#Variables globales
argv0: string;

##############################################
#Excepciones
usage()
{
	fprint(sys->fildes(2), "uso: %s <direccion_paginas1> [...direccion_paginasN] <direccion_repartidor>\n", argv0);
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

	for(i=0; i<listaServers.nTotal; i++)
	{
		if (i==0)
		{
			sys->print("--------------- Broadcast ------------------------\n");
			#sys->print("Nº peticiones de broadcast : %s \n", string listaServers.server[i].nPeticiones);
			sys->print("------------------------------------------------\n");
		}else{
			sys->print("--------------- Servidor %s -----------------------\n", string (i));
			sys->print("Direccion server : %s \n", listaServers.server[i].direccionServer);
			sys->print("------------------------------------------------\n");
		}
	}
}
###############################################


#Inicializa una estructura de tInfoServers
##################################################
inicializartInfoServers(listaServers : ref tInfoServers, nTotal : int)
# - Inicializa una estructura de tInfoServers
##################################################
{
	i: int;

	#Guardamos el nTotal (nº de argv -1)
	#Le sumamos 1 mas!. La pos 0 se reserva para broadcast
	listaServers.nTotal= nTotal + 1;

	#Instanciamos el array...¡aqui se le da tamaño!
	listaServers.server = array [listaServers.nTotal] of tServer;
	
	#Empezamos en 0. Al llamar a poltica ya correremos una posicion
	listaServers.sig=0;
	
	for(i=0; i<listaServers.nTotal; i++)
	{
		listaServers.server[i].direccionServer = nil;
	}
}
##################################################



###############################################
conectarConServers(listaServers : ref tInfoServers)
# - Realiza la conexion inicial con los servers
###############################################
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
			#listaServers.server[i].estado = -1;
			sys->print("¡falló la conexion! \n");
		}else{
			sys->print("ok! \n");
		}
	}
}


##########################################################
politica (listaServers : ref tInfoServers, canal_peticiones: chan of tPeticionPolitica)
# - Actualiza el nº de server que atenderá la peticion (usamos round-robin)
# - Se comunica por canales, para evitar condiciones de carrera.
# - Recibe el canal por el que se espera la respuesta. Devuelve un ack
##########################################################
{
	peticionPolitica: tPeticionPolitica;

	for(;;)
	{
		sys->print("repartidor->Thread_politica: esperando a recibir peticiones\n");
		peticionPolitica=<-canal_peticiones;

		if(listaServers.nTotal!=0)
		{
			#Si es el último, damos la vuelta
			if (listaServers.sig == (listaServers.nTotal -1))
			{
				listaServers.sig = 1;
			}else{
				listaServers.sig++;
			}
			sys->print("repartidor->Thread_politica: enviando respuesta <ok>\n");
			peticionPolitica.canal_respuesta<-="ok";
			

		}else{
			sys->print("repartidor->Thread_politica: enviando respuesta <no hay servers>\n");
			listaServers.sig=-1;
			peticionPolitica.canal_respuesta<-="no hay servers";
		}
	}
}

###############################################

#######################################################
tratar_cliente ( conexCliente : ref FD, listaServers : ref tInfoServers, canal_conexiones: chan of int,
			canal_politica: chan of tPeticionPolitica)
# - Se encarga de gestionar todas las peticiones de un cliente
#######################################################
{
	#Variables de gestion de conexión
	peticionCliente : array of byte;
	es: string;
	e1, e2 :int;
	peticionUnpack : ref Reqmsg;

	#Variables para coordinacion de actualizaciones (etapa8)
	peticionLiberacionUnpack : ref Reqmsg;
	recursoLiberado : string;
	peticionLiberacion : array of byte;

	#Variables para evitar condiciones de carrera al hacer round robin
	peticionPolitica :tPeticionPolitica;
	respuestaPolitica: string;

	for(;;)
	{
		pintartInfoServers(listaServers);

		sys->print("repartidor->tratar_cliente: listo para leer peticiones de cliente\n");
		#Leemos la peticion del cliente, para no tener q volver a leer si falla el server.
		(peticionCliente, es) = readmsg(conexCliente, 10*TAM_BLOQUE);

		#Controlamos q se leyera correctamente
		if (es != nil)
		{
			#OJO, VER POSIBILIDADES exit/return!!!!
			sys->print("repartidor->tratar_cliente: Fin de conexion con cliente...exit\n");
			sys->print("------------------------------------------\n");
			canal_conexiones<-=-1;
			exit;
			#end of connection
			#return;
		}else{
		
			#Desempaquetamos, ya que necesitamos saber el tipo de peticion
			(peticionUnpack, es) = Reqmsg.unpack(peticionCliente);
			if (es!=nil)
			{
				sys->print("repartidor->tratar_cliente: Error al desempaquetar...exit\n");
				exit;
			}else{
			
				#Tenemos que tomar la estructura, en funcion de su tipo
				pick r := peticionUnpack {
					Exit =>
						#respuesta = ref Repmsg.Exit();
						sys -> print("repartidor->tratar_cliente: Recibida peticion de finalización\n");
						sys-> print("repartidor->tratar_cliente: Fin de conexion con cliente...exit\n");
						sys-> print("------------------------------------------\n");
						#salir=1;
						canal_conexiones <-= -1;
						exit;


					Get =>
						#Volvemos a empaquetar, una vez conocido el tipo
						peticionCliente = peticionUnpack.pack();

						#Round-Robin: preguntamos al thread por el siguiente server. 
						#Usamos canales, ya que se dan condiciones de carrera
						sys->print("repartidor->tratar_cliente: enviando peticion a thread politica\n");
						peticionPolitica.canal_respuesta = chan of string;
						canal_politica <- = peticionPolitica;
						respuestaPolitica=<-peticionPolitica.canal_respuesta;
						#Respuesta de threadPolitica
						sys->print("repartidor->tratar_cliente: El thread nos ha respondido :%s\n", respuestaPolitica);
						do
						{
							sys->print("repartidor->tratar_cliente: Enviando peticion GET a %s <pos %s> \n", listaServers.server[listaServers.sig].direccionServer, string listaServers.sig);
							#listaServers.server[listaServers.sig].nPeticiones++;

							e1 =reqdb(peticionCliente, listaServers.server[listaServers.sig].conexionServer, fildes(2));
							if (e1<0)
							{
								sys->print("repartidor->tratar_cliente: Error en el reqdb\n !");
							}else{

								e2 = repdb(listaServers.server[listaServers.sig].conexionServer, conexCliente, fildes(2));		
								if (e2<0)
								{
									sys->print("repartidor->tratar_cliente: Error en el repdb \n");
								}
							}


							#Si hubo algun error, tenemos q volver a escoger server
							if ((e1<0)||(e2<0))
							{
								sys->print("repartidor->tratar_cliente: ¡El servidor %s parece caído!, vamos a elegir otro\n", string listaServers.sig);
								#Preguntamos al thread por el siguiente server. 
								#Usamos el mismo canal
								sys->print("repartidor->tratar_cliente: enviando peticion a thread politica\n");
								#OJO!peticionPolitica.canal_respuesta = chan of string;
								canal_politica <- = peticionPolitica;
								respuestaPolitica=<-peticionPolitica.canal_respuesta;
								#Respuesta de threadPolitica
								sys->print("repartidor->tratar_cliente: El thread nos ha respondido <inBucle>:%s \n", respuestaPolitica);

							}
				
						}while( ( (e1<0) || (e2<0) ) && (listaServers.sig!=-1) );

						#Si no hay ningun server, forzamos la salida
						if (listaServers.sig==-1)
						{
							sys->print("repartidor->tratar_cliente: Abortada ejecución de repartidor, por falta de servidores activos...bye!\n");
							exit;
						}

					Put =>
						sys->print("repartidor->tratar_cliente: Solicitada actualización. Enviando el PUT a todos los servers \n");
					
						recursoLiberado =  r.name;
						#Volvemos a empaquetar, una vez conocido el tipo
						peticionCliente = peticionUnpack.pack();

						#Si es un put, se lo enviaremos a todos
						sendBroadcast(peticionCliente,listaServers,fildes(2));
						sys->print("repartidor->tratar_cliente: enviados todos los puts\n");
						#Recogemos todos los ack, pero respondemos solo una vez al cliente
						receivePutBroadcast(listaServers,conexCliente,fildes(2));
						sys->print("repartidor->tratar_cliente: Recogidos todos los ack de puts\n");

						#MODIFICACIONES ETAPA 8
						#Ahora tenemos que hacer un broadcast de liberacion de ese recurso
						#Preparamos la peticion de liberacion
						peticionLiberacionUnpack= ref Reqmsg.End(recursoLiberado);
						peticionLiberacion = peticionLiberacionUnpack.pack();
						
						#Enviamos a todos
						sendBroadcast(peticionLiberacion,listaServers,fildes(2));
						sys->print("repartidor->tratar_cliente: enviados todos los ends\n");
						#Recoge todos los ack, pero no responde a cliente!
						receiveEndBroadcast(listaServers,fildes(2));
						sys->print("repartidor->tratar_cliente: Recogidos todos los ack de ends\n");

					}#pick
						#################################################
			}#Control_desempaquetar
		}#Control_lectura
	}#Bucle infinito
}	

#######################################################


##############################################################
acceptcli(c: Sys->Connection): ref FD
# - Acepta conexiones de un cliente, en el descriptor de conexion del repartidor
##############################################################
{
	(es, ccon) := listen(c);
	if (es < 0)
		raise sprint("fail: can't listen: %r");
	ccon.dfd = open(ccon.dir + "/data", ORDWR);
	if (ccon.dfd == nil)
		raise sprint("fail: can't open connection: %r");
	return ccon.dfd;
}
###########################################################

##############################################################
reqdb(buffer_cliente: array of byte, out: ref FD, dbg: ref FD) : int
# - Muestra el contenido de la peticion del cliente, y se la envia a un paginas
# - Le pasamos el array de bytes. Leemos la peticion en el PP!
# - Devuelve -1 si hubo algún error, 1 en caso contrario
##############################################################
{

	sys->print("repartidor->reqdb: inciando envio simple \n");
	(m, es) := Reqmsg.unpack(buffer_cliente);

	#Controlamos que se desempaquetara correctamente
	if (es != nil)
	{
		fprint(dbg, "repartidor->reqdb: req: readmsg: %s", es);
		return -1;
		exit;
	}

	
	fprint(dbg, "repartidor->reqdb: cli→srv: %s\n", m.text());
	#Si todo fue correcto, se lo enviamos (un write al fd_de la conex del server)
	wl := write(out, buffer_cliente, len buffer_cliente);
	if (wl != len buffer_cliente)
	{
		fprint(dbg, "repartidor->reqdb: req: can't write msg: %r\n");
		return -1;
		exit;
	}else{
		return 1;
	}
}




###############################################################
repdb(in: ref FD, out: ref FD, dbg: ref FD) : int
# - Hace un receive de la respuesta del paginas, lo muestra, y se la envia al cliente
# - Seguimos pasandole la conexion!
###############################################################
{
	sys->print("repartidor->repdb: iniciando recepcion simple\n");
	(buf, e) := readmsg(in, 10*1024);


	#Controlamos q se leyera correctamente
	if (e != nil)
	{
		fprint(dbg, "repartidor->repdb: rep: readmsg: %s", e);
		return -1;
		exit;
	}

	(m, es) := Repmsg.unpack(buf);

	#Controlamos que se desempaquetara correctamente
	if (es != nil)
	{
		fprint(dbg, "repartidor->repdb: rep: readmsg: %s", es);
		return -1;
		exit;
	}


	fprint(dbg, "repartidor->repdb: srv→cli: %s\n", m.text());

	#Si todo fue correcto, se lo enviamos (un write al fd_de la conex del cliente)
	wl := write(out, buf, len buf);
	if (wl != len buf)
	{
		fprint(dbg, "repartidor->repdb: rep: can't write msg: %r\n");
		return -1;
		exit;
	}else{
		return 1;
	}
}
###############################################################



###################################################################
sendBroadcast(buffer_cliente: array of byte, listaServers: ref tInfoServers, dbg: ref FD)
# - Muestra el contenido de la peticion del cliente, y se la envia a todos los paginas
# - Le pasamos el buffer de peticion directamente
# - Tiene acceso a la ED, asi q si detecta algun server caido; lo anotará
# - Nueva modificacacion: accede a listaServers solo en modo lectura...no anota
#    el que el server este caido
###################################################################
{
	i: int;

	(m, es) := Reqmsg.unpack(buffer_cliente);

	#Controlamos que se desempaquete correctamente
	if (es != nil)
	{
		fprint(dbg, "repartidor->sendBroadcast : req: readmsg: %s", es);
		exit;
	}

	sys->print("Comenzando ENVIO BROADCAST...\n");
	sys->print("----------------------------------------------------------------\n");
	for(i=1; i<listaServers.nTotal; i++)
	{
		fprint(dbg, "cli→srv: %s\n", m.text());
		sys->print("repartidor->sendBroadcast : Enviando peticion a %s <pos %s> ...", listaServers.server[i].direccionServer, string i);
		#Si todo fue correcto, se lo enviamos (un write al fd_de la conex del server)
		wl := write(listaServers.server[i].conexionServer, buffer_cliente, len buffer_cliente);
		
		if (wl != len buffer_cliente)
		{
			fprint(dbg, "repartidor->sendBroadcast : req: can't write msg: %r\n");
			sys->print("DESCUBIERTO SERVER CAIDO...PERO NO ANOTAMOS!\n");
			#Exit??????
		}else{
			sys->print("ok\n");
		}
	}
	sys->print("----------------------------------------------------------------\n");
}
###################################################################

##############################################################
receivePutBroadcast(listaServers: ref tInfoServers, out: ref FD, dbg: ref FD)
# - Hace un receive de todos los servers, y muestra su contenido
# - Tiene acceso a listaServers en lectura, si ve un servidor caido no espera respuesta
# - Envia el contenido del primero que funciona al cliente
##############################################################
{
	i: int;
	esPrimeraVez : int;
	bufferAux : array of byte;
	hayAlguno : int;

	esPrimeraVez = -1;
	hayAlguno = -1;


	sys->print("repartidor->receivePutBroadcast: iniciando receive ack PUT de todos los servers\n");
	sys->print("----------------------------------------------------------------\n");
	#Recogemos tantas peticiones como nServers
	for (i=1; i<listaServers.nTotal; i++)
	{

		(buf, e) := readmsg(listaServers.server[i].conexionServer, 10*TAM_BLOQUE);

		#Controlamos q se leyera correctamente
		if (e != nil)
		{
			fprint(dbg, "repartidor->receivePutBroadcast : rep: readmsg: %s", e);
			#exit;????
		}else{

			sys->print("repartidor->receivePutBroadcast : recepecion broadcast de %s < pos %s >,...OK\n", listaServers.server[i].direccionServer, string i);

			#Recogemos contenido del primero que funcione...¡pero no enviamos aun!
			if (esPrimeraVez<0)
			{
				esPrimeraVez = 1;
				hayAlguno = 1;
				bufferAux = buf;
			}
		}
	}

	#Si hubo alguno que respondio, enviamos la respuesta
	if (hayAlguno>0)
	{
		sys->print("repartidor->receivePutBroadcast : Enviando ack PUT al cliente...\n");
		wl := write(out, bufferAux, len bufferAux);
		if (wl != len bufferAux)
		{
			fprint(dbg, "repartidor->receivePutBroadcast : rep: can't write msg: %r\n");
			#exit;???
		}
	}else{
		sys->print("repartidor->receivePutBroadcast : Ningún server contesto!. no podemos contestar a cliente! \n");
	}
	sys->print("----------------------------------------------------------------\n");
}
############################################################## 


##############################################################
receiveEndBroadcast(listaServers: ref tInfoServers, dbg: ref FD)
# - Hace un receive de todos los servers, y muestra su contenido
# - Accede a la listaServers en modo lectura, si lo ve caido lo descarta
# - No envia nada al cliente!
##############################################################
{
	i: int;

	sys->print("repartidor->receiveEndBroadcast: iniciando receive ack END de todos los servers\n");
	sys->print("----------------------------------------------------------------\n");
	#Recogemos tantas peticiones como nServers
	for (i=1; i<listaServers.nTotal; i++)
	{
		(buf, e) := readmsg(listaServers.server[i].conexionServer, 10*1024);

		#Controlamos q se leyera correctamente
		if (e != nil)
		{
			fprint(dbg, "repartidor->receiveEndBroadcast: rep: readmsg: %s", e);
				#exit;????
		}else{
			sys->print("repartidor->receiveEndBroadcast: receive broadcast de %s < pos %s >, ...OK\n",listaServers.server[i].direccionServer, string i);
		}
	}
	sys->print("----------------------------------------------------------------\n");
}
############################################################## 

############################################
netmkaddr(addr, net, svc: string): string
# - Prepara el formato de la conexion
############################################
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
############################################


#################################################
connect(dest: string): ref FD
# - Realiza la conexion con un paginas, dada su direccion 
# - Devuelve el descriptor de la conexion
#################################################
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
#################################################


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

		sys->print("repartidor->thread checkea_conexiones: esperando a recibir peticiones\n");
		aux = <- canal_entrada;
		sys->print("repartidor->thread checkea_conexiones: recibido el valor -> %s\n", string aux);
		nConexiones = nConexiones + aux;

		#Si el nº de conexiones es 0...lanzo al thread asesino
		if(nConexiones==0) 
		{
			spawn killemall(gid);
		}else{
			#Si no...mato a todo el grupo de control
			#Abrimos el descriptor, y le escribimos señal killgrp
			fd:=open("/prog/" + string gid2 + "/ctl", OWRITE);
			if (fd==nil)
			{
				sys->print("repartidor->thread checkea_conexiones: Descriptor de grupo vacio!\n");
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
	sys->print("repartidor->killemall : Me duermo 15 sg...\n");
	sleep(15000);
	sys->print("repartidor->killemall : voy a matar todos los procesos\n");

	#Abrimos el descriptor, y le escribimos señal killgrp
	fd:=open("/prog/" + string gid + "/ctl", OWRITE);
	if (fd==nil)
	{
		sys->print("repartidor->killemall: Descriptor de grupo vacio\n");
	}else{
		write(fd, array of byte "killgrp", 7);	
	}
	exit;
}
###################################################################



#######################################################
#######################################################
#Programa principal
#######################################################
init(nil: ref Draw->Context, argv: list of string)
{
	#Declaracion de variables
	nServers : int;
	i : int;
	direccRepartidor : string;
	conexCliente : ref FD;
	gid : int;

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

	#Y los canales de conexiones y politica
	canal_conexiones:= chan of int;
	canal_politica:= chan of tPeticionPolitica;

	#Agrupamos los threads de grupo
	gid = pctl(NEWPGRP, nil);
	
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

	#y guardamos nuestra direccion de repartidor
	direccRepartidor  = hd argv;
	########################################################

	#Hacemos la conexion inicial con los paginas
	conectarConServers(listaServers);
	pintartInfoServers(listaServers);
	
	#Creamos el thread que gestiona el n_conexiones y el que gestiona round robin
	spawn checkea_conexiones(canal_conexiones,gid);
	spawn politica(listaServers, canal_politica);

	#aceptamos las conexiones de los clientes
	(e, c) := announce(direccRepartidor);
	if (e < 0)
	{
		raise sprint("error al realizar la llamada a announce %r");
	}

	sys->print("repartidor->init: repartidor arrancado correctamente en %s\n", direccRepartidor);
	sys->print("repartidor->init: las peticiones se repartiran entre : \n");
	pintartInfoServers(listaServers);

	for(;;)
	{
		sys->print("repartidor-> init : esperando por clientes...\n");
		#Hacemos un socket por cliente, y le damos un thread
		conexCliente = acceptcli(c);
		#Enviamos un 1 al thread de gestion de conexiones
		canal_conexiones<-=1;
		#¿Condiciones de carrera en listaServers?
		spawn tratar_cliente(conexCliente, listaServers, canal_conexiones, canal_politica);
		
	}
}


