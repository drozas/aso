#########################################################
# PRACTICA ASO : Servidor web distribuido
# ---------------------------------------
# Fase actual : 8 (Coordinación de actualizaciones)
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
			sys->print("Nº peticiones de broadcast : %s \n", 
				  string listaServers.server[i].nPeticiones);
			sys->print("------------------------------------------------\n");
		}else{
			sys->print("--------------- Servidor %s -----------------------\n", string (i));
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
##################################################
inicializartInfoServers(listaServers : ref tInfoServers, nTotal : int)
#Inicializa una estructura de tInfoServers
##################################################
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
##################################################



###############################################
conectarConServers(listaServers : ref tInfoServers)
# - Intenta conectar con todos los servidores almacenados
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
			listaServers.server[i].estado = -1;
			sys->print("¡falló la conexion! \n");
		}else{
			listaServers.server[i].estado = 1;
			sys->print("ok! \n");
		}
	}
}


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
###############################################


##########################################################
politica (listaServers : ref tInfoServers)
# - Actualiza el nº de server que atenderá la peticion (usamos round-robin)
# - Controla que no escojamos los que ya sabemos que han caído
##########################################################
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
				sys->print("La peticion será atendida por ---> %s <<pos %s>>\n", 
						   listaServers.server[listaServers.sig].direccionServer, 
						   string listaServers.sig);
				encontrado =1;
			}else{
				sys->print("Servidor descartado, estaba caido---> %s \n",
						   listaServers.server[listaServers.sig].direccionServer);
			}
		}while(encontrado<0);
			

	}else{
		sys->print("¡¡¡¡¡¡ No hay ningún servidor activo !!!!!\n");
		listaServers.sig=-1;
	}
}

###############################################

#######################################################
tratar_cliente ( conexCliente : ref FD, listaServers : ref tInfoServers)
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

	pintartInfoServers(listaServers);

	#Leemos la peticion del cliente, para no tener q volver a leer si falla el server.
	(peticionCliente, es) = readmsg(conexCliente, 10*1024);

	#Controlamos q se leyera correctamente
	if (es != nil)
	{
		sys->print("error al leer la petición del cliente %s \n", es);
		exit;
	}else{
		
		#Desempaquetamos, ya que necesitamos saber el tipo de peticion
		(peticionUnpack, es) = Reqmsg.unpack(peticionCliente);
		if (es!=nil)
		{
			sys->print("Error al desempaquetar\n");
		}else{
			
			#Tenemos que tomar la estructura, en funcion de su tipo
			pick r := peticionUnpack {
				Get =>
					#Si es un get, hacemos un round robin como antes
					#Volvemos a empaquetar, una vez conocido el tipo
					peticionCliente = peticionUnpack.pack();

					#Llamaremos para escoger un server
					politica(listaServers);
					#Con este bucle, detectamos nuevos servidores caidos
					do
					{
						sys->print("Enviando peticion GET a %s <pos %s> \n", 
					  		 listaServers.server[listaServers.sig].direccionServer, string listaServers.sig);
						listaServers.server[listaServers.sig].nPeticiones++;

						e1 =reqdb(peticionCliente, listaServers.server[listaServers.sig].conexionServer, fildes(2));
						if (e1<0)
						{
							sys->print("Error en el reqdb\n !");
						}else{

							e2 = repdb(listaServers.server[listaServers.sig].conexionServer, conexCliente, fildes(2));		
							if (e2<0)
							{
								sys->print("Error en el repdb \n");
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

				Put =>
					listaServers.server[0].nPeticiones++;
					sys->print("Solicitada actualización. Enviando el PUT a todos los servers \n");
					
					recursoLiberado =  r.name;
					#Volvemos a empaquetar, una vez conocido el tipo
					peticionCliente = peticionUnpack.pack();

					#Si es un put, se lo enviaremos a todos
				
					#Esta funcion se lo envia a todos, y detecta servers caidos
					sendBroadcast(peticionCliente,listaServers,fildes(2));
					#Esta funcion recoge todos los ack, pero responde solo una vez al cliente
					receivePutBroadcast(listaServers,conexCliente,fildes(2));
					sys->print("Recogidos todos los ack de puts\n");

					#MODIFICACIONES ETAPA 8
					#Ahora tenemos que hacer un broadcast de liberacion de ese recurso
					#Preparamos la peticion de liberacion
					peticionLiberacionUnpack= ref Reqmsg.End(recursoLiberado);
					peticionLiberacion = peticionLiberacionUnpack.pack();
					#Enviamos a todos, y detectamos servers caidos
					sendBroadcast(peticionLiberacion,listaServers,fildes(2));
					#Recoge todos los ack, pero no responde a cliente!
					receiveEndBroadcast(listaServers,fildes(2));
					sys->print("Recogidos todos los ack de ends\n");

				}#pick
					#################################################
		}#Control_desempaquetar
	}#Control_lectura
}	

#######################################################


###########################################################
acceptcli(addr: string): ref FD
# - Acepta conexiones de un cliente, en la direccion indicada
###########################################################
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
###########################################################

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




###############################################################
repdb(in: ref FD, out: ref FD, dbg: ref FD) : int
# - Hace un receive de la respuesta del paginas, lo muestra, y se la envia al cliente
# - Seguimos pasandole la conexion!
###############################################################
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
###############################################################



###################################################################
sendBroadcast(buffer_cliente: array of byte, listaServers: ref tInfoServers, dbg: ref FD)
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

	sys->print("Comenzando ENVIO BROADCAST...\n");

	for(i=1; i<listaServers.nTotal; i++)
	{
		fprint(dbg, "cli→srv: %s\n", m.text());
		sys->print("Enviando peticion a %s <pos %s> ...", 
				   listaServers.server[i].direccionServer, string i);
		#Si todo fue correcto, se lo enviamos (un write al fd_de la conex del server)
		wl := write(listaServers.server[i].conexionServer, buffer_cliente, len buffer_cliente);
		
		if (wl != len buffer_cliente)
		{
			fprint(dbg, "req: can't write msg: %r\n");
			sys->print("¡se ha caido! \n");
			sys->print("Marcando server %s  <pos %s> como inactivo \n",
					  listaServers.server[i].direccionServer, string i);
			listaServers.server[i].estado==-1;
			#exit;
		}else{
			sys->print("ok\n");
		}
		
	}
}
###################################################################

##############################################################
receivePutBroadcast(listaServers: ref tInfoServers, out: ref FD, dbg: ref FD)
# - Hace un receive de todos los servers, y muestra su contenido
# - Tiene acceso a la ED, asi q si detecta algun server caido; lo anotará
# - Envia el contenido del primero que funciona al cliente
##############################################################
{
	i: int;
	esPrimeraVez : int;
	bufferAux : array of byte;
	hayAlguno : int;

	esPrimeraVez = -1;
	hayAlguno = -1;

	#Recogemos tantas peticiones como nServers
	for (i=1; i<listaServers.nTotal; i++)
	{

		#Controlamos que el server este vivo
		if(listaServers.server[i].estado==1)
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


			fprint(dbg, "paginas→repartidor: %s\n", m.text());

			#Recogemos contenido del primero que funcione...¡pero no enviamos aun!
			if (esPrimeraVez<0)
			{
				esPrimeraVez = 1;
				hayAlguno = 1;
				bufferAux = buf;
			}

		}else{
			sys->print("Descartada recepecion broadcast de %s < pos %s >, ya que esta caido",
					   listaServers.server[i].direccionServer, string i);
		}	

	}

	#Si hubo alguno que respondio, enviamos la respuesta
	if (hayAlguno>0)
	{
		sys->print("Enviando ack al cliente...\n");
		wl := write(out, bufferAux, len bufferAux);
		if (wl != len bufferAux)
		{
			fprint(dbg, "rep: can't write msg: %r\n");
			exit;
		}
	}else{
		sys->print("Ningún server contesto!. no podemos contestar a cliente! \n");
	}
}
############################################################## 


##############################################################
receiveEndBroadcast(listaServers: ref tInfoServers, dbg: ref FD)
# - Hace un receive de todos los servers, y muestra su contenido
# - Tiene acceso a la ED, asi q si detecta algun server caido; lo anotará
# - No envia nada al cliente!
##############################################################
{
	i: int;
	bufferAux : array of byte;

	#Recogemos tantas peticiones como nServers
	for (i=1; i<listaServers.nTotal; i++)
	{

		#Controlamos que el server este vivo
		if(listaServers.server[i].estado==1)
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

			#Y mostramos info por pantalla
			fprint(dbg, "paginas→repartidor: %s\n", m.text());


		}else{
			sys->print("Descartado el receive broadcast a %s < pos %s >, ya que esta caido",
					   listaServers.server[i].direccionServer, string i);
		}	

	}

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

	for(;;)
	{
		tratar_cliente(conexCliente, listaServers);

	}
}


