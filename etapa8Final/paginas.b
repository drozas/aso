#########################################################
# PRACTICA ASO : Servidor web distribuido
# ---------------------------------------
# Fase actual : 8 (Coordinacion de actualizaciones)
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
	listen, open, read, write, sprint, ORDWR, FD : import sys;
include "draw.m";

include "wp.m";
	wp: Wp;
	readmsg, Reqmsg, Repmsg, writemsg : import wp;


#Constantes
MAX_PAGS : con 2;
MAX_BYTES : con 1024;

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
	sig : int;
	max : int;
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

	#Instanciamos el array...¡aqui se le da tamaño inicial!
	listaPags.pag = array [MAX_PAGS] of tPag;
	listaPags.max = MAX_PAGS;
	
	#y el resto de la estructura
	for(i=0; i<MAX_PAGS; i++)
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

	for(i=0; i<listaPags.sig; i++)
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
	while (i<listaPags.sig && encontrada==-1)
	{
		if (listaPags.pag[i].ruta==ruta) 
		{
			sys->print("pagina encontrada en cache en la pos %s \n", string i);
			#Cogemos el valor de la pos, q es lo q devolveremos
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
	if (listaPags.sig == listaPags.max) 
	{
		sys->print("array de cache desbordado...aumentando posiciones!!!\n");
		#Si no, creamos un nuevo array con una pos mas
		arrayAux := array[listaPags.max+1] of tPag;
		#Copiamos el contenido del anterior
		for(i=0; i<listaPags.max; i++)
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
		listaPags.max++;
		
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
	


############################################################
servirGet(listaPags : ref tCachePags, ruta : string) : (string, int)
# - Devuelve el contenido de un fichero
# - Actualiza la cache, si es la primera vez que lo piden
# - Devuelve -1 en en caso de error de apertura (no existencia)
# - Controla que no se haya podido acceder a la pagina en cache porque
#    este siendo actualizada. En ese caso devuelve -2
############################################################
{
	contenido : string;
	nBytesLeidos : int;
	buffer := array[MAX_BYTES] of byte;
	desc_fichero : ref FD;
	enCache : int;
	error : int;

	
	desc_fichero=open (ruta,Sys->OREAD);
	error = 1;
		
	#Notificamos en caso de error de apertura	
	if (desc_fichero==nil)
	{
		sys-> print("error al abrir el fichero <%s>\n", ruta);
		#Si hubo error devolvemos -1
		error=-1;
		contenido = nil;
	}else{	
		
		#Miramos si esta en cache
		enCache = estaEnCache(listaPags,ruta);
		if (enCache != -1)
		{
			#MODIFICACIONES ETAPA 8
			sys->print("la pagina solicitada ya estaba en cache!\n");
			if(listaPags.pag[enCache].bloqueada>0)
			{
				sys->print("estaba en cache, y no estaba bloqueada\n");
				contenido= listaPags.pag[enCache].contenido;
			}else{
				sys->print("estaba en cache...¡¡¡pero esta bloqueada!!!!\n");
				error = -2;
			}
		}else{
		
			#Si no, leemos de disco
			sys->print("no estaba en cache. leyendo de disco <%s>\n", ruta);

	 		contenido ="";
			do
			{
				nBytesLeidos = 0;
				#buffer_peticion [0:] = nil;
					
				nBytesLeidos = read (desc_fichero,buffer,MAX_BYTES);
				sys -> print("Se han leido %s bytes \n", string nBytesLeidos);

				if (nBytesLeidos > 0)
				{
					#guardamos si leimos algo
					contenido = contenido + string buffer[0:nBytesLeidos];
				}else if(nBytesLeidos<0){
					#notificamos en caso de error
					sys->print("error al leer un bloque \n");
					error = -1;
				}
	
			#Saldremos si es 0 (nada q leer) o negativo (hubo error)
			}while (nBytesLeidos>0);

			# y a partir de ahora la guardamos en cache
			sys->print("guardando la pagina solicitada en cache...");
			#error2= insertarPag(listaPags, ruta, contenido);
			insertarPag(listaPags, ruta, contenido);

			#if (error2<0) 
			#{
				#sys->print("no fue posible, la cache esta llena \n");
			#}else{
				#sys->print("ok \n");
			#}	
			
			
			
		}#Cache/no cache
	}#Existe/no_existe

	return (contenido,error);
}
###################################################################




###################################################################
servirPut (listaPags : ref tCachePags, ruta : string, nuevoContenido :  string) : int
# - Modifica el contenido de un archivo, tanto en cache como en disco
# - Lo guarda en cache si es la primera vez que lo piden
# - Devuelve -1 en en caso de error de apertura (no existencia)
# - Esta es la funcion que realiza el bloqueo en si
# - Posibles cambios: invertir control sistema_fich/cache ?
############################################################
{
	nBytesEscritos : int;
	desc_fichero : ref FD;
	error: int;
	enCache : int;

	error = 1;
	nBytesEscritos = 0;
		
	#abrimos el fichero en modo escritura
	desc_fichero = open(ruta,sys->OTRUNC|sys->OWRITE);
			
	#comprobamos la existencia del fichero
	if(desc_fichero==nil)
	{
		#Si no lo servimos, devolvemos error
		sys->print("El fichero a actualizar  no es servido\n");
		error = -1;
	}else{
		#Si el fichero existe, escribimos el contenido que nos pasan
		nBytesEscritos = write(desc_fichero, array of byte (nuevoContenido),
						   len( array of byte (nuevoContenido)));
		#y controlamos que se escribio algo
		if(nBytesEscritos==-1)
		{
			sys->print("Error en el proceso de escritura del fichero");
			error = -1;
		}else{
			sys->print("Se han escrito %s bytes en %s \n", string nBytesEscritos, ruta);
			#Por ultimo, solo nos queda actualizar la cache
			enCache= estaEnCache(listaPags,ruta);
			if (enCache<0) 
			{
				#MODIFICACIONES ETAPA 8
				sys->print("el fichero no constaba en cache, agregando en cache (y bloqueando)...");
				#error2=insertarPag(listaPags, ruta, nuevoContenido);
				insertarPag(listaPags, ruta, nuevoContenido);
				#OJITO!!!!!
				listaPags.pag[(listaPags.sig)-1].bloqueada==-1;
				#ESto....habra q hacerlo dinamico...asi no nos preocuparemos...!!!
				#if (error2<0)
				#{
				#	sys->print("cache llena!. El contenido sera actualizado solo en el fichero");
				#}else{
				#	sys->print("ok");
				#}

			}else{
				#MODIFICACIONES ETAPA 8
				#Si estaba en cache, tenemos que ver si estaba bloqueada ¿¿deshacer cambios en fich??
				if (listaPags.pag[enCache].bloqueada>0)
				{
					sys->print("estaba en cache, pero no estaba bloqueada. cambiamos cont y bloqueamos\n");
					listaPags.pag[enCache].contenido= nuevoContenido;
					listaPags.pag[enCache].bloqueada = -1;
				}else{
					sys->print("estaba en cache, y estaba bloqueada....habra q guardar peticion!\n");
					error=-2;
				}
			
			}#cache/no_cache

		}#existe/no_existe
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
accesoCache( listaPags : ref tCachePags, canal_peticion: chan of tPeticionCache)
# - Hilo que se encarga de gestionar el acceso a la cache
# - Realiza las peticiones get/put 
# - Recibe peticiones por un unico canal, pero devuelve el resultado a cada thread
#     en su canal correspondiente
# - Esta es la funcion que implementa la capacidad desbloqueante
###################################################################	
{
	peticionCache : tPeticionCache;
	respuestaCache : tRespuestaCache; 
	contenido : string;
	error : int;

	#Variables para gestion de bloqueos (et8)
	peticionesPendientes, listaAux, listaInversa : list of tPeticionCache;
	pos : int;
	petiAux : tPeticionCache;

	for(;;)
	{
		#Leemos peticiones de threads de cliente por el canuto de peticiones
		sys->print("esperando a recibir peticiones de acceso a cache <<THREAD accesoCache>>\n");
		peticionCache = <- canal_peticion;
	
		sys->print("Recibida peticion ...\n");
		pintartPeticionCache(peticionCache);
		sys->print("-----------------\n");

		#Analizamos la peticion, en funcion de su tipo
		case peticionCache.tipoPeticion {
			"get" => 
				sys-> print("tratando peticion GET de %s ENEL THREAD DE CACHE\n", peticionCache.ruta);
				#Llamamos a nuestra antigua funcion servirGet, con la info de la peticion
				(contenido,error) = servirGet(listaPags, peticionCache.ruta);
				#Hacemos control de errores exclusivamente de peticiones bloqueadas
				if (error == -2)
				{
					sys->print("pagina bloqueada...hay que guardar peticion get!\n");
					peticionesPendientes= peticionCache::peticionesPendientes;
				}else{
					sys->print("la pag no esta bloqueada, enviaremos respuesta a thread tratar_cliente \n");	
					respuestaCache.tipoRespuesta= "get";
					respuestaCache.contenido= contenido;
					respuestaCache.error = error;
					#Enviamos la respuesta por el canal de respuesta(de cada thread!)
					sys -> print("ENVIANDO RESPUESTA A UN GET DESDE THREAD DE CACHE \n");
					peticionCache.canalRespuesta <- = respuestaCache;
				}
			

			"put" =>
				sys-> print("tratando peticion PUT de %s ENEL THREAD DE CACHE\n", peticionCache.ruta);
				#Llamamos a nuestra antigua funcion servirPut, con la info de la peticion
				error = servirPut(listaPags,peticionCache.ruta,peticionCache.nuevoContenido);
				#Hacemos control de errores solo de peticiones bloqueadas
				if (error==-2)
				{
					sys->print("pagina bloqueada...hay que guardar peticion put!\n");
					peticionesPendientes = peticionCache::peticionesPendientes;

				}else{
					respuestaCache.tipoRespuesta= "put";
					respuestaCache.contenido= nil;
					respuestaCache.error = error;
					#Enviamos la respuesta por el canal de respuesta(de cada thread!)
					sys -> print("ENVIANDO RESPUESTA A UN GET DESDE THREAD DE CACHE \n");
					peticionCache.canalRespuesta <- = respuestaCache;
				}


			"end" =>
				#Solo desbloquea, la liberacion de recursos se produce abajo
				sys-> print("tratando peticion END de %s ENEL THREAD DE CACHE\n", peticionCache.ruta);
				#Miramos si hay alguna pag en cache con ese nombre para desbloquear
				pos = estaEnCache(listaPags,peticionCache.ruta);
				if(pos<0)
				{
					sys->print("Nos han hecho un end de una pagina que no tenemos en cache\n");
					respuestaCache.tipoRespuesta="end";
					respuestaCache.contenido = nil;
					respuestaCache.error= -1;
					#Enviamos la respuesta por el canal de respuesta(de cada thread!)
					sys -> print("ENVIANDO RESPUESTA A UN END ERRONEO DESDE THREAD DE CACHE \n");
	
				}else{
					sys->print("Se ha encontrado una pagina a desbloquear!!!\n");
					#Lo primero, es cambiarle el estado
					listaPags.pag[pos].bloqueada=1;

					#Contestamos al thread de cliente q nos paso el end
					respuestaCache.tipoRespuesta="end";
					respuestaCache.contenido = nil;
					respuestaCache.error= 1;
					#Enviamos la respuesta por el canal de respuesta(de cada thread!)
					sys -> print("ENVIANDO RESPUESTA A UN END DESDE THREAD DE CACHE \n");
				}
			
				peticionCache.canalRespuesta <- = respuestaCache;

		}#Case

		#Liberacion de peticiones pendientes
		#Proceso...
		# - Separo mi lista en dos...las que voy a poder tratar, y las que no
		# - Las que no, sera la nueva lista en la proxima transicion (listaAux)
		# - Las que si, invierto el orden de la lista, y envio a saco (listaInversa)
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
					sys->print("Reguardo en bloqueadas!\n");
				}else{
					#Si no, en la lista de las q podremos tratar, q ya tendra el orden correcto
					listaInversa = petiAux::listaInversa;
					sys->print("Reguardo en no bloqueadas!\n");
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

		sys->print("############Lista de pendientes <tiene que estar en orden inverso>\n");
		pintarListaPeticiones(peticionesPendientes);
		sys->print("########## Lista de la que vamos a tratar <tiene q estar en buen orden>\n");
		pintarListaPeticiones(listaInversa);


		#Tratamos las peticiones que si podemos atender (ya las tenemos en el orden correcto)
		while(listaInversa!=nil)
		{
			petiAux = hd listaInversa;
			listaInversa = tl listaInversa;

			case petiAux.tipoPeticion {
				"get" =>

					sys-> print("tratando peticion GET de %s PENDIENTE\n", peticionCache.ruta);
					#Llamamos a nuestra antigua funcion servirGet, con la info de la peticion
					(contenido,error) = servirGet(listaPags, petiAux.ruta);
					#Hacemos control de errores exclusivamente de peticiones bloqueadas
					#Tenemos que seguir haciendolo porque puede haber un put de por medio!
					if (error == -2)
					{
						#Si hubo un put de por medio, la volvemos a meter
						sys->print("pagina bloqueada...la volvemos a meter\n");
						peticionesPendientes= petiAux::peticionesPendientes;
					}else{
						sys->print("la pag no esta bloqueada, enviaremos respuesta a thread tratar_cliente \n");	
						respuestaCache.tipoRespuesta= "get";
						respuestaCache.contenido= contenido;
						respuestaCache.error = error;
						#Enviamos la respuesta por el canal de respuesta(de cada thread!)
						sys -> print("ENVIANDO RESPUESTA A UN GET DESDE THREAD DE CACHE \n");
						petiAux.canalRespuesta <- = respuestaCache;
					}
				"put" =>
						
					sys-> print("tratando peticion PUT de %s PENDIENTE\n", peticionCache.ruta);
					#Llamamos a nuestra antigua funcion servirPut, con la info de la peticion
					error = servirPut(listaPags,petiAux.ruta,petiAux.nuevoContenido);
					#Hacemos control de errores solo de peticiones bloqueadas
					if (error==-2)
					{
						sys->print("pagina bloqueada...hay que guardar peticion again put!\n");
						peticionesPendientes = petiAux::peticionesPendientes;

					}else{
						respuestaCache.tipoRespuesta= "put";
						respuestaCache.contenido= nil;
						respuestaCache.error = error;
						#Enviamos la respuesta por el canal de respuesta(de cada thread!)
						sys -> print("ENVIANDO RESPUESTA A UN GET DESDE THREAD DE CACHE \n");
						petiAux.canalRespuesta <- = respuestaCache;
					}


				}
			



			}			
	
	}
}



#############################################################
tratar_cliente(conex_cliente: ref FD, canal_peticion : chan of tPeticionCache)
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
	
	sys->print("Recibido nuevo cliente... \n");
	for(;;){
		salir=0;

		#Leemos el buffer de la peticion
		(buffer_peticion, error_s) = readmsg(conex_cliente, 10*1024);
		
                	#Controlamos si hubo error al leer el mensaje, q sera el fin de la conexion
		if (error_s!=nil)
		{
			sys->print("Fin de conexion con cliente\n");
			sys->print("------------------------------------------\n");
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

		#Tratamiento en funcion del tipo de peticion (reg.variable)
                	pick r := peticion {
			Exit =>
				respuesta = ref Repmsg.Exit();
				sys -> print("Recibida peticion de finalización\n");
				sys-> print("Fin de conexion con cliente\n");
				sys-> print("------------------------------------------\n");
				salir=1;
			Get =>
				
				sys-> print("recibida peticion get de %s, en thread <<tratar_cliente>>\n", r.name);
			
				#Preparamos la peticion de acceso al thread que controla la cache
				peticion_Cache.tipoPeticion= "get";
				peticion_Cache.ruta = r.name;
				peticion_Cache.nuevoContenido = nil;
				#y aqui hacemos la instancia!
				peticion_Cache.canalRespuesta = chan of  tRespuestaCache;
				
				#Send & receive con el thread de cache
				sys-> print("enviando peticion a thread de cache\n");
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

			Put =>
				sys->print("Recibida peticion put de %s \n. Enviando al thread de cache", r.name);

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

			End =>
				sys->print("Recibida peticion tipo End, de momento no la tratamos\n");
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

				if(respuesta_Cache.error >0)
				{
					#Si todo fue correcto, devolvemos ack en instancia tipoEnd
					respuesta = ref Repmsg.End();
				}else{
					#Si no, devolvemos instancia tError 
					respuesta =  ref Repmsg.Error("no existe");
				}
			* =>
				respuesta = ref Repmsg.Error("peticion invalida");
			}

			#Empaquetamiento del mensaje y envio
               	 	writemsg(conex_cliente, respuesta.pack());
		
               	 if(salir)
		{
			sys->print("Se desconecto un cliente\n");
			exit;
		}
	}
}


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
	cont_aux : string;
	
	#Declaramos es instanciamos la cache a la vez
	listaPags := ref tCachePags;
	#Y el canal en el que el thread espeara peticiones de acceso a cache
	canal_peticion := chan of tPeticionCache;

	#En esta funcion se instanciara el array interno!
	inicializartCachePags(listaPags);
	pintartCachePags(listaPags);

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
		
		#utilizamos la llamada a get, para refrescar la cache en demanda
		(cont_aux,error) = servirGet(listaPags, rutaAux);
		if ( error<0 ) 
		{
			sys->print("Error al insertar %s en la cache. El fichero no existe. \n", rutaAux);
		}else{
			sys->print("Se inserto correctamente %s en la cache \n", rutaAux);
		}
	}
	
	pintartCachePags(listaPags);
        #####################################################
	
	#Y creamos el thread que gestiona la cache
	spawn accesoCache(listaPags,canal_peticion);	

	# Inicializacion de la conexion
	#####################################################
	#announce translate a given addr to an actual network address using the connection server
  	#para realizarla correctamente-> "tcp!*!" + puerto
	direccion_red= "tcp!*!" + puerto;
	(error, conexion) = announce(direccion_red);

	if (error < 0)
	{
		raise sprint("error: se realizo incorrectamente la llamada a announce: %r");
	}else{
		sys->print("\n######## Inicializacion de paginas ###########\n");
              	sys->print("---> Puerto :  %s \n", puerto);
		sys->print("---> Tamaño maximo de cache: %s paginas\n", string MAX_PAGS);
		sys->print("########################################\n\n");	
        }
        #####################################################
		
        for(;;){
		{
		ccon: Sys->Connection;
		sys->print("Esperando clientes...\n");

		#Creamos un nuevo socket para cada cliente
		(error, ccon) = listen(conexion);
		if (error < 0)
		{
			raise sprint("error: se realizo incorrectamente la llamada a listen %r");
		}
           	
		ccon.dfd = open(ccon.dir + "/data", ORDWR);
	
		if (ccon.dfd == nil)
		{
			raise sprint("error: no se pudo realizar correctamente la llamada a open de la conexion: %r");
		}

		spawn tratar_cliente(ccon.dfd, canal_peticion);
		}exception error2{
			* =>
				;
		}

	}
}
#####################################################################
