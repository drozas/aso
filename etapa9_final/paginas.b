#########################################################
# PRACTICA ASO : Servidor web distribuido
# ---------------------------------------
# Fase actual : 9 ("Tolerando caidas del repartidor"). Version final
# Fichero : paginas.b
# Descripcion : Sirve peticiones de paginas a partir de su cache, o del 
#                         sistema de ficheros. Utiliza el protocolo WP
# Autor : David Rozas
#########################################################

implement Paginas;

Paginas: module
{
	PATH	: con "./paginas.dis";	
	init	: fn(nil: ref Draw->Context, argv: list of string);
};

include "sys.m";
	sys: Sys;
	fprint, fildes, announce,
	listen, open, read, write, sprint, ORDWR, FD, sleep, pctl,
	OWRITE, NEWPGRP : import sys;
include "draw.m";

include "wp.m";
	wp: Wp;
	readmsg, Reqmsg, Repmsg, writemsg : import wp;


#Constantes
TAM_INICIAL : con 1;
TAM_BLOQUE : con 1024;


#Definicion de tipos para la gestion de la cache
tPag : adt
{
	ruta : string;
	contenido : string;
	bloqueada : int;
};

tCachePags : adt
{
	pag : array of tPag;
	sig : int; #campo utilizado para gestionar round-robin
	nTotal : int;
};
##############################################

#Definicion de tipos para el protocolo de acceso a cache
tPeticionCache : adt
{
	canalRespuesta : chan of tRespuestaCache;
	tipoPeticion : string;
	ruta : string;
	nuevoContenido : string;
};

tRespuestaCache : adt
{
	tipoRespuesta : string;
	error : int;
	contenido : string;
};
##############################################

#Variables globales
argv0: string;


##############################################


#Excepciones 
usage()
{
	fprint(fildes(2), "uso: %s <puerto> [<fich1> ... <fichN>]\n", argv0);
	raise "fail: usage";
}


##########################################################
inicializartCachePags(listaPags : ref tCachePags)
# - Inicializa una estructura de tCachePags
##########################################################
{
	i: int;

	#Inicializamos el indice
	listaPags.sig= 0;

	#Instanciamos el array
	listaPags.pag = array [TAM_INICIAL] of tPag;
	listaPags.nTotal = TAM_INICIAL;
	
	#y el resto de la estructura
	for(i=0; i<TAM_INICIAL; i++)
	{
		listaPags.pag[i].ruta = nil;
		listaPags.pag[i].contenido = nil;
		listaPags.pag[i].bloqueada = 0;
	}
}
##########################################################



##########################################################
pintartCachePags(listaPags : ref tCachePags)
# - Muestra por pantalla el contenido de la cache
##########################################################
{
	i: int;

	sys->print("Nº total de paginas en cache : %s \n", string (listaPags.sig));

	for(i=0; i<listaPags.nTotal; i++)
	{
		sys->print("--------------- Pagina %s---------\n", string (i + 1));
		sys->print("Ruta : %s \n", listaPags.pag[i].ruta);
		sys->print("Contenido : %s \n", listaPags.pag[i].contenido);
		sys->print("Bloqueada por actualizacion? : ");
		if(listaPags.pag[i].bloqueada>0)
		{
			sys->print("No\n");
		}else{
			sys->print("Si\n");
		}
		sys->print("---------------------------------\n");
	}
}
##########################################################


#####################################################
pintartPeticionCache(peti : tPeticionCache)
# - Muestra por pantalla el contenido de una tPeticionCache (trazas)
#####################################################
{
	sys->print("@@@@@@@@@@@@@@@@@@@@@@@@@@\n");
	sys->print("tipo_peticion : %s \n", peti.tipoPeticion);
	sys->print("ruta : %s \n", peti.ruta);
	sys->print("nuevo_contenido (solo puts) : %s \n", peti.nuevoContenido);
	sys->print("@@@@@@@@@@@@@@@@@@@@@@@@@@\n");
}
#####################################################


##########################################################
estaEnCache(listaPags : ref tCachePags, ruta : string) : int
# - Busca en cache la pagina solicitada.
# - Si la encuentra devuelve su posicion, si no devuelve -1.
##########################################################
{
	encontrada, i : int;

	i = 0;
	encontrada = -1;

	#Recorremos hasta encontrarla o si ya recorrimos todo
	while (i<listaPags.nTotal && encontrada==-1)
	{
		if (listaPags.pag[i].ruta==ruta) 
		{
			sys->print("pagina encontrada en cache en la pos %s \n", string i);
			#Cogemos el valor de la pos, que es lo que devolveremos
			encontrada = i;
		}
		
		i++;
	}
	
	return encontrada;
}
############################################################



############################################################
insertarPag(listaPags : ref tCachePags, ruta : string, contenido : string)
# - Guarda la informacion de un nodo pagina
# - Si hemos excedido el limite, copia el contenido en un nuevo array mayor
#    y añade el nuevo nodo
############################################################
{
	i: int;


	#Comprobamos que aun queda espacio
	if (listaPags.sig == listaPags.nTotal) 
	{
		#Si no, creamos un nuevo array con una pos mas
		arrayAux := array[listaPags.nTotal+1] of tPag;
		#Copiamos el contenido del anterior
		for(i=0; i<listaPags.nTotal; i++)
		{
			arrayAux[i].ruta = listaPags.pag[i].ruta;
			arrayAux[i].contenido = listaPags.pag[i].contenido;
			arrayAux[i].bloqueada = listaPags.pag[i].bloqueada;
		}

		#Hacemos que la lista apunte al nuevo array
		listaPags.pag = arrayAux;
		#Y agregamos el nuevo
		listaPags.pag[listaPags.sig].ruta = ruta;
		listaPags.pag[listaPags.sig].contenido = contenido;
		listaPags.pag[listaPags.sig].bloqueada = 1;
		#Actualizamos indice y nuevo maximo
		listaPags.sig++ ;
		listaPags.nTotal++;
		
	}else{
		#Guardamos la info
		listaPags.pag[listaPags.sig].ruta = ruta;
		listaPags.pag[listaPags.sig].contenido = contenido;
		listaPags.pag[listaPags.sig].bloqueada = 1;
		#Actualizamos indice
		listaPags.sig++ ;
	}
	
}
############################################################
	


###################################################################
leerPagDisco(ruta: string): (string,int)
# - Lee un fichero de disco, y devuelve -1 si hubo algun error
###################################################################
{
	
	descFichero: ref FD;
	error: int;
	contenido: string;
	nBytesLeidos: int;
	buffer:= array[TAM_BLOQUE] of byte;

	descFichero= open(ruta, Sys->OREAD);
	if (descFichero!=nil)
	{
		#Leemos en bloques 
		contenido="";
		error = 1;
		do
		{
			nBytesLeidos=0;
			nBytesLeidos= read(descFichero,buffer,TAM_BLOQUE);

			if (nBytesLeidos>0)
			{
				contenido= contenido+ string buffer[0:nBytesLeidos];
			}else if (nBytesLeidos<0){
				sys->print("paginas->leerPagDisco: error al leer un bloque!\n");
				error=-1;
			}
		}while(nBytesLeidos>0);
	}else{
		error=-1;
		sys->print("paginas->leerPagDisco: la pagina no existe!\n");
	}

	return(contenido,error);
}
###################################################################

###################################################################
escribirPagDisco(ruta: string, contenido: string):int
# - Escribe el contenido en el fichero indicado. Devuelve -1 en caso de error
###################################################################
{
	nBytesEscritos: int;
	descFichero: ref FD;
	error: int;
	
	error=1;

	descFichero= open(ruta, sys->OTRUNC | sys->OWRITE);
	if(descFichero!=nil)
	{
		nBytesEscritos=write(descFichero, array of byte (contenido), len (array of byte(contenido)));
		if (nBytesEscritos<0)
		{
			sys->print("paginas->escribirPagDisco: error en el proceso de escritura!\n");
			error=-1;
		}

	}else{
		error=-1;
		sys->print("paginas->escribirPagDisco: la pagina no existe!\n");
	}

	return error;
}			

		
################################################################
pintarListaPeticiones ( listaPags : list of tPeticionCache)
# - Muestra por pantalla el contenido de una lista de peticiones pendientes (trazas)
################################################################
{
	while (listaPags!=nil)
	{
		pintartPeticionCache(hd listaPags);
		listaPags = tl listaPags;
	}
}	
################################################################

###################################################################
lease(pos: int, canal_lease: chan of int)
# - Hilo que se encarga de avisar que se libere una pagina bloqueada, tras 15sg
###################################################################
{
	#Me duermo 15sg, y devuelvo la posicion que me pasaron
	sleep(15000);
	canal_lease <-=pos;
}
###################################################################

###################################################################		
accesoCache( listaPags : ref tCachePags, canal_peticion: chan of tPeticionCache)
# - Hilo que se encarga de gestionar el acceso a la cache
# - Realiza las peticiones get/put 
# - Recibe peticiones por un unico canal, pero devuelve el resultado a cada thread
#     en su canal correspondiente
# - Esta es la funcion que implementa la capacidad bloqueante y desbloqueante
###################################################################	
{
	peticionCache : tPeticionCache;
	respuestaCache : tRespuestaCache; 
	contenido : string;
	error : int;
	enCache: int;

	#Variables para gestion de bloqueos (et8)
	peticionesPendientes, listaAux, listaInversa : list of tPeticionCache;
	pos : int;
	petiAux : tPeticionCache;

	#Variables gestion de lease (et9)
	posLease : int; #Recoge el valor leido en el canal_lease
	canal_lease := chan of int;

	for(;;)
	{
		#Leemos peticiones de threads de cliente por el canuto de peticiones
		sys->print("paginas->accesoCache : esperando a recibir peticiones...\n");


		alt
		{
			#Peticiones de threads tratar_cliente
			peticionCache = <- canal_peticion =>
			sys->print("$$$$$$$$$ TRATANDO PETICION CLIENTE EN THREAD CACHE$$$$$$$$$$$$$$$$$$\n");
			{
	
				sys->print("paginas->accesoCache : Recibida peticion de acceso a cache...\n");
				pintartPeticionCache(peticionCache);

				#Analizamos la peticion, en funcion de su tipo
				case peticionCache.tipoPeticion {
					"get" => 
						sys-> print("paginas->accesoCache : tratando un get de %s \n", peticionCache.ruta);
			
						#Tratamiento de peticion get
						###############################################
						#Primero vemos si la tenemos en cache
						enCache= estaEnCache(listaPags,peticionCache.ruta);
						if (enCache !=-1)
						{
							sys->print("paginas->accesoCache: la tenemos en cache\n");
							if(listaPags.pag[enCache].bloqueada==1)
							{
								sys->print("paginas->accesoCache: estaba en cache, y no estaba bloqueada. Enviaremos la respuesta al thread tratar_cliente\n");
								respuestaCache.tipoRespuesta="get";
								respuestaCache.contenido= listaPags.pag[enCache].contenido;
								respuestaCache.error=1;
								#Enviamos la respuesta por el canal de ese thread
								peticionCache.canalRespuesta <- = respuestaCache;
							}else{
								sys->print("paginas->accesoCache: estaba en cache, pero estaba bloqueada. Guardamos peticion get\n");
								peticionesPendientes= peticionCache::peticionesPendientes;
							}

						}else{
							sys->print("paginas->accesoCache: no estaba en cache, vamos a intentar leer de disco\n");
							(contenido,error)=leerPagDisco(peticionCache.ruta);
							if(error>0)
							{
								sys->print("paginas->accesoCache:lectura de disco correcta, añadiremos pag. a cache\n");
								insertarPag(listaPags,peticionCache.ruta,contenido);
							}else{
								sys->print("paginas->accesoCache: lectura de disco incorrecta, no almacenamos en cache, y avisaremos a cliente\n");
							}

								respuestaCache.tipoRespuesta="get";	
								respuestaCache.contenido=contenido;
								respuestaCache.error= error;
								peticionCache.canalRespuesta <- = respuestaCache;
						}
						######### Fin tratamiento get ###########################
	
	
					"put" =>
						sys-> print("paginas->accesoCache : tratando peticion put de %s\n", peticionCache.ruta);

						######## Tratamiento put ###########################
						#Primero buscamos si la tenemos en cache
						enCache=estaEnCache(listaPags,peticionCache.ruta);
						if(enCache!=-1)
						{
							sys->print("paginas->accesoCache: la tenemos en cache\n");
							if(listaPags.pag[enCache].bloqueada==1)
							{
								sys->print("paginas->accesoCache: estaba en cache, y no estaba bloqueada. Enviaremos la respuesta al thread tratar_cliente\n");
								#La bloqueamos 
								listaPags.pag[enCache].bloqueada=-1;

								#Refrescamos contenido en cache
								sys->print("paginas->accesoCache: refrescando contenido en cache...\n");
								listaPags.pag[enCache].contenido= peticionCache.nuevoContenido;
								#Y refrescamos contenido en disco
								sys->print("paginas->accesoCache: refrescando contenido en disco...\n");
								error=escribirPagDisco(peticionCache.ruta,peticionCache.nuevoContenido);

								#Por último, respondemos al thread tratar_cliente
								respuestaCache.tipoRespuesta="put";
								respuestaCache.contenido= nil;
								respuestaCache.error=error;
								peticionCache.canalRespuesta <- = respuestaCache;

								sys ->print("paginas->accesoCache: lanzando thread lease //b.inicial// para pos %s\n", string enCache);
								#Y lanzamos un thread de lease con esa pos
								spawn lease(enCache, canal_lease);

							}else{
								sys->print("paginas->accesoCache: estaba en cache, pero estaba bloqueada. Guardamos peticion put\n");
								peticionesPendientes= peticionCache::peticionesPendientes;
							}


						}else{
							sys->print("paginas->accesoCache: no esta en cache\n");
						
							#Refrescamos contenido en disco
							sys->print("paginas->accesoCache: refrescando contenido en disco...\n");
							error=escribirPagDisco(peticionCache.ruta,peticionCache.nuevoContenido);
							#Y si existe, la guardamos en cache
							if(error>0)
							{
								sys->print("paginas->accesoCache: refrescando contenido en cache...\n");
								insertarPag(listaPags,peticionCache.ruta,peticionCache.nuevoContenido);	
							}else{
								sys->print("paginas->accesoCache: error al escribir en disco, avisaremos al cliente...\n");
							}
							#Respondemos al thread tratar_cliente
							respuestaCache.tipoRespuesta="put";
							respuestaCache.contenido= nil;
							respuestaCache.error=error;
							peticionCache.canalRespuesta <- = respuestaCache;

							#Y lanzamos un thread de lease con esa pos
							enCache= estaEnCache(listaPags,peticionCache.ruta);
							sys ->print("paginas->accesoCache: lanzando thread lease //b.inicial// para pos %s\n", string enCache);
							spawn lease(enCache, canal_lease);

						}
						########### Fin tratamiento put #################

					"end" =>
						sys-> print("paginas->accesoCache: tratando peticion end de %s\n", peticionCache.ruta);
						########### Tratamiento end #####################
						pos = estaEnCache(listaPags,peticionCache.ruta);
						if(pos<0)
						{
							sys->print("paginas->accesoCache:Nos han hecho un end de una pagina que no tenemos en cache\n");
							#Preparamos la respuesta
							respuestaCache.tipoRespuesta="end";
							respuestaCache.contenido = nil;
							respuestaCache.error= -1;
	
						}else{
							sys->print("paginas->accesoCache: Se ha encontrado una pagina a desbloquear!!!\n");
							#Lo primero, es cambiarle el estado
							listaPags.pag[pos].bloqueada=1;

							#Preparamos la respuesta
							respuestaCache.tipoRespuesta="end";
							respuestaCache.contenido = nil;
							respuestaCache.error= 1;
						}
						
						sys->print("paginas->accesoCache: respondiendo peticion end a init\n");
						peticionCache.canalRespuesta <- = respuestaCache;

				}#Case
				sys->print("$$$$$$$$$ FIN DE  PETICION EN THREAD CACHE$$$$$$$$$$$$$$$$$$\n");
			}#alt_canal_peticiones
		
			#Peticiones de thread lease
			posLease =<-canal_lease =>
			{
				sys->print("paginas->accesoCache :  recibida peticion de desbloqueo LEASE \n");
				#Si estaba bloqueada la desbloqueo, si no...es que ya habra llegado el end
				if(listaPags.pag[posLease].bloqueada==-1)
				{
					sys->print("paginas->accesoCache :pagina en pos : %s estaba bloqueada ...haremos efectivo el lease!!!!\n", string posLease);
					listaPags.pag[posLease].bloqueada=1;
				}else{
					sys->print("paginas->accesoCache :pagina en pos : %s NO estaba bloqueada, despreciaremos el lease!!!", string posLease);
				}

			}
		}#estructura_alt

		sys->print("/////////////////////////////////////////////////////////////\n");
		sys->print("//////////// ESTADO DE LA CACHE (antes de liberacion)///////////\n");
		sys->print("/////////////////////////////////////////////////////////////\n");
		pintartCachePags(listaPags);
		sys->print("/////////////////////////////////////////////////////////////\n");


		######################################################################
		#Liberacion de peticiones pendientes: tanto por llegada de ends, como por lease
		#Proceso...
		# - Separo mi lista en dos: las que voy a poder tratar, y las que no
		# - Las que no, sera la nueva lista en la proxima transicion (listaAux)
		# - Las que si, invierto el orden de la lista, y envio  (listaInversa)
		#############################################################
		listaAux = nil;
		listaInversa=nil;
		while(peticionesPendientes!=nil)
		{
			petiAux = hd peticionesPendientes;
			pos= estaEnCache(listaPags,petiAux.ruta);
			if (pos>-1)
			{
				#Si sigue bloqueada, la guardo en lAux, q se invertira
				if(listaPags.pag[pos].bloqueada==-1)	
				{
					listaAux= petiAux::listaAux;
				}else{
					#Si no, en la lista de las q podremos tratar, q ya tendra el orden correcto
					listaInversa = petiAux::listaInversa;
				}
			}else{
				sys->print("Reservada peticion que no esta en cache\n");
			}
			peticionesPendientes =  tl peticionesPendientes;

		}

		#La nueva lista a tratar, será la de las que no vamos a poder atender (hay q reInvertirla)
		peticionesPendientes =  nil;
		while(listaAux!=nil)
		{
			peticionesPendientes = (hd listaAux)::peticionesPendientes;
			listaAux= tl listaAux;
		}

		sys->print("############Lista de pendientes <tiene que estar en orden inverso de llegada>\n");
		pintarListaPeticiones(peticionesPendientes);
		sys->print("########## Lista de la que vamos a tratar <tiene q estar en orden correcto de llegada>\n");
		pintarListaPeticiones(listaInversa);


		#Tratamos las peticiones que si podemos atender (ya las tenemos en el orden correcto)
		while(listaInversa!=nil)
		{
			petiAux = hd listaInversa;
			listaInversa = tl listaInversa;

			case petiAux.tipoPeticion {
				"get" =>

					sys-> print("tratando peticion GET de %s PENDIENTE\n", petiAux.ruta);
		
					#Primero la localizamos en cache
					enCache= estaEnCache(listaPags,petiAux.ruta);
					if (enCache !=-1)
					{
						sys->print("paginas->accesoCache: localizada peticion pendiente\n");
						if(listaPags.pag[enCache].bloqueada==1)
						{
								sys->print("paginas->accesoCache: ya esta desbloqueada. Enviaremos la respuesta al thread tratar_cliente\n");
								respuestaCache.tipoRespuesta="get";
								respuestaCache.contenido= listaPags.pag[enCache].contenido;
								respuestaCache.error=1;
								#Enviamos la respuesta por el canal de respuesta(de cada thread!)
								petiAux.canalRespuesta <- = respuestaCache;
							}else{
								sys->print("paginas->accesoCache: sigue bloqueada!\n");
								peticionesPendientes= petiAux::peticionesPendientes;
							}

					}else{
						sys->print("esto no debe pasar!!!!!!!\n");
					}


					##########################################################
				"put" =>
						
					sys-> print("tratando peticion PUT de %s PENDIENTE\n", petiAux.ruta);
		
					#Primero la localizamos en cache
					enCache= estaEnCache(listaPags,petiAux.ruta);
					if(enCache!=-1)
					{
						sys->print("paginas->accesoCache: localizada peticion pendiente\n");
						if(listaPags.pag[enCache].bloqueada==1)
						{

							#La bloqueamos 
							listaPags.pag[enCache].bloqueada=-1;

							#Refrescamos contenido en cache
							sys->print("paginas->accesoCache: refrescando contenido en cache...\n");
							listaPags.pag[enCache].contenido= petiAux.nuevoContenido;

							#Y refrescamos contenido en disco
							sys->print("paginas->accesoCache: refrescando contenido en disco...\n");
							error=escribirPagDisco(petiAux.ruta,petiAux.nuevoContenido);

							#Preparamos la respuesta
							sys->print("paginas->accesoCache: ya esta desbloqueada\n");
							respuestaCache.tipoRespuesta="put";
							respuestaCache.contenido= nil;
							respuestaCache.error=error;
							#Enviamos la respuesta al thread correspondiente
							petiAux.canalRespuesta <- = respuestaCache;

							sys ->print("paginas->accesoCache: lanzando thread lease //b.secundario// para pos %s\n", string enCache);
							#Y lanzamos un thread de lease con esa pos
							spawn lease(enCache, canal_lease);
						}else{
								sys->print("paginas->thread_cache: sigue bloqueada!\n");
								peticionesPendientes= petiAux::peticionesPendientes;
						}


					}else{
						sys->print("esto no deber pasar!!!!!!!!\n");
					}

				}#case
			}#while			
			################### Fin tratamiento peticiones pendiente ###############	
	
		sys->print("/////////////////////////////////////////////////////////////\n");
		sys->print("/////////// ESTADO DE LA CACHE (despues de liberacion)//////////\n");
		sys->print("/////////////////////////////////////////////////////////////\n");
		pintartCachePags(listaPags);
		sys->print("/////////////////////////////////////////////////////////////\n");

	}#bucle infinito
}



#############################################################
tratar_cliente(conex_cliente: ref FD, canal_peticion : chan of tPeticionCache, canal_conexiones: chan of int)
# - Hilo que se encarga de tratar todas las peticiones de un cliente
#############################################################
{
        	salir : int;
	buffer_peticion: array of byte;
	error_s: string;

	#Var. aux para peticion/recepcion con el thread de cache
	peticion_Cache : tPeticionCache;
	respuesta_Cache : tRespuestaCache;

	#Variables para recoger el contenido desempaquetado
	peticion : ref Reqmsg;
	re : string;
	
	#Variables para envio de respuesta
	respuesta : ref Repmsg;
	
	sys->print("paginas->tratar_cliente: Recibido nuevo cliente... \n");
	for(;;){
		salir=0;

		#Leemos el buffer de la peticion
		(buffer_peticion, error_s) = readmsg(conex_cliente, 10*1024);
		
                	#Controlamos si hubo error al leer el mensaje, q sera el fin de la conexion
		if (error_s!=nil)
		{
			sys->print("paginas->tratar_cliente: Fin de conexion con cliente\n");
			sys->print("------------------------------------------\n");
			canal_conexiones<-=-1 ;
			#end of connection
			return;
		}

                 #Si esta vacio, levantamos una excepcion
		if (buffer_peticion == nil)
		{
			raise "nil msg";
		}
	
		#Desempaquetamos la peticion
		(peticion, re) = Reqmsg.unpack(buffer_peticion);

             	#Controlamos si hubo error al desempaquetar
		if (re != nil)
		{	
                	raise sprint("fail: can't unpack: %s", re);
		}

		#Tratamiento en funcion del tipo de peticion 
                	pick r := peticion {
			Exit =>
				respuesta = ref Repmsg.Exit();
				sys -> print("paginas->tratar_cliente: Recibida peticion de finalización\n");
				sys-> print("paginas->tratar_cliente: Fin de conexion con cliente\n");
				sys-> print("------------------------------------------\n");
				salir=1;
			Get =>
				
				sys-> print("paginas->tratar_cliente: recibida peticion get de %s\n", r.name);
				sys->print("**************Inicio de peticion GET *******************\n");
			
				#Preparamos la peticion de acceso al thread que controla la cache
				peticion_Cache.tipoPeticion= "get";
				peticion_Cache.ruta = r.name;
				peticion_Cache.nuevoContenido = nil;
				#y aqui hacemos la instancia!
				peticion_Cache.canalRespuesta = chan of  tRespuestaCache;
				
				#Send & receive con el thread de cache
				sys-> print("paginas->tratar_cliente: enviando peticion a thread acceso_cache\n");
				canal_peticion <- = peticion_Cache;
				respuesta_Cache = <-  peticion_Cache.canalRespuesta;
	
				if (respuesta_Cache.error>0) 
				{				
					#Si todo fue correcto, devolvemos el cotenido en instancia de tipoGet
					respuesta = ref Repmsg.Get(respuesta_Cache.contenido);
				}else{
					#Si no, devolvemos instancia de tipoError
					respuesta = ref Repmsg.Error("no existe");
				}
				sys->print("************ Fin de peticion GET******************\n");

			Put =>
				sys->print("paginas->tratar_cliente: Recibida peticion put de %s \n", r.name);
				sys->print("************ Inicio de peticion PUT******************\n");
				#Preparamos la peticion de acceso al thread que controla la cache
				peticion_Cache.tipoPeticion= "put";
				peticion_Cache.ruta = r.name;
				peticion_Cache.nuevoContenido = r.content;
				#y aqui hacemos la instancia!
				peticion_Cache.canalRespuesta = chan of tRespuestaCache;
				
				#Send & receive con el thread de cache
				canal_peticion <- = peticion_Cache;
				respuesta_Cache = <-  peticion_Cache.canalRespuesta;
				

				if(respuesta_Cache.error >0)
				{
					#Si todo fue correcto, devolvemos ack en instancia tipoPut
					respuesta = ref Repmsg.Put();
				}else{
					#Si no, devolvemos instancia tError 
					respuesta =  ref Repmsg.Error("no existe");
				}				
				sys->print("************ Fin de peticion PUT******************\n");
			End =>
				sys->print("paginas->tratar_cliente: Recibida peticion tipo End\n");
				sys->print("************ Inicio de peticion END ******************\n");
				#Preparamos la peticion de acceso al thread que controla la cache
				peticion_Cache.tipoPeticion= "end";
				peticion_Cache.ruta = r.name;
				peticion_Cache.nuevoContenido = nil;
				#y aqui hacemos la instancia!
				peticion_Cache.canalRespuesta = chan of tRespuestaCache;
				
				#Send & receive con el thread de cache
				canal_peticion <- = peticion_Cache;
				respuesta_Cache = <-  peticion_Cache.canalRespuesta;
				respuesta = ref Repmsg.End();
				sys->print("************ Fin de peticion END******************\n");
			* =>
				respuesta = ref Repmsg.Error("peticion invalida");
			}

			#Empaquetamiento del mensaje y envio
               	 	writemsg(conex_cliente, respuesta.pack());
		
               	 if(salir)
		{
			sys->print("paginas->tratar_cliente: Se desconecto un cliente\n");
			canal_conexiones <- = -1;
			exit;
		}
	}
}

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

		sys->print("paginas->checkea_conexiones: esperando a recibir peticiones\n");
		aux = <- canal_entrada;
		sys->print("paginas->checkea_conexiones: recibido el valor -> %s\n", string aux);
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
				sys->print("paginas->checkea_conexiones : Descriptor de grupo vacio!\n");
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
	sys->print("paginas->killemall : Me duermo 15 sg...\n");
	sleep(15000);
	sys->print("paginas->killemall: Voy a matar a todos\n");

	#Abrimos el descriptor, y le escribimos señal killgrp
	fd:=open("/prog/" + string gid + "/ctl", OWRITE);
	if (fd==nil)
	{
		sys->print("paginas->killemall: Descriptor de grupo vacio!\n");
	}else{
		write(fd, array of byte "killgrp", 7);	
	}
	exit;
}
###################################################################

###################################################################
###################################################################
#Programa Principal
###################################################################
init(nil: ref Draw->Context, argv: list of string)
{

	#Carga e inicializacion de modulos
        	sys = load Sys Sys->PATH;
	wp  = load Wp Wp->PATH;
	wp->setup();


        	#Declaracion de variables locales
        	rutaAux, puerto, direccion_red: string;
        	error: int;
        	conexion: Sys ->Connection;
	contenidoAux : string;
	gid : int;
	
	#Declaramos es instanciamos la cache a la vez
	listaPags := ref tCachePags;
	#Y los canales de los threads de gestion de cache y check_conexiones
	canal_peticion := chan of tPeticionCache;
	canal_conexiones := chan of int;

	#Con esto podemos matar todos los threads del grupo
	gid = pctl(NEWPGRP, nil);

	#En esta funcion se instanciara el array interno!
	inicializartCachePags(listaPags);

        #Recogemos el primer argumento (nombre del ejecutable)
	argv0 = hd argv;
	#y llamamos a tail, para eliminar el argumento recogido anteriormente
	argv  = tl argv;

        # Recogida de parametros
        #####################################################
	#Controlamos que el numero de argumentos restantes sea al menos el puerto
	if (len argv <1)
		usage();

	#Recogemos el puerto
        	puerto = hd argv;
        	argv = tl argv;
        

	#y los ficheros que vamos a servir inicialmente por solicitud del prompt.
	while (argv != nil)
	{
		rutaAux = hd argv;
		argv = tl argv;
		
		#Leemos de disco, e insertamos en cache
		(contenidoAux,error)= leerPagDisco(rutaAux);
		if ( error<0 ) 
		{
			sys->print("paginas->init: El fichero %s no existe. No se insertara en cache \n", rutaAux);
		}else{
			sys->print("paginas->init: Insertando %s en la cache \n", rutaAux);
			insertarPag(listaPags, rutaAux, contenidoAux);
		}
	}
	
        #####################################################
	
	#Y creamos el thread que gestiona la cache y el q controla el n_conexiones
	spawn accesoCache(listaPags,canal_peticion);	
	spawn checkea_conexiones(canal_conexiones, gid);

	# Inicializacion de la conexion
	#####################################################
	#announce translate a given addr to an actual network address using the connection server
	direccion_red= "tcp!*!" + puerto;
	(error, conexion) = announce(direccion_red);

	if (error < 0)
	{
		raise sprint("paginas->init: se realizo incorrectamente la llamada a announce: %r");
	}else{
		sys->print("paginas->init : paginas arrancado en puerto %s\n", string puerto);
        }
        #####################################################
		
        for(;;){
		{
		ccon: Sys->Connection;
		sys->print("paginas->init : Esperando clientes...\n");

		#Creamos un nuevo socket para cada cliente
		(error, ccon) = listen(conexion);
		if (error < 0)
		{
			raise sprint("paginas->init : error: se realizo incorrectamente la llamada a listen %r");
		}
           	
		ccon.dfd = open(ccon.dir + "/data", ORDWR);
	
		if (ccon.dfd == nil)
		{
			raise sprint("paginas->init : error: no se pudo realizar correctamente la llamada a open de la conexion: %r");
		}
		#Le enviamos un 1 al thread de gestion de conexiones
		canal_conexiones<-=1;
		spawn tratar_cliente(ccon.dfd, canal_peticion, canal_conexiones);
		}exception error2{
			* =>
				;
		}

	}
}
#####################################################################
