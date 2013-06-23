#########################################################
# PRACTICA ASO : Servidor web distribuido
# ---------------------------------------
# Fase actual : 2 (servidor multithread)
# Fichero : paginas.b
# Descripcion : Sirve un string usando WP
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
MAX_PAGS : con 10;
MAX_BYTES : con 1024;

#Definicion de tipos
tPag : adt
{
	ruta : string;
	contenido : string;
};

tCachePags : adt
{
	pag : array of tPag;
	sig : int;
};


#Variables globales
argv0: string;

usage()
{
	fprint(fildes(2), "uso: %s <puerto> [<fich1> ... <fichN>]\n", argv0);
	raise "fail: usage";
}

#Inicializa una estructura de tCachePags
##########################################################
inicializartCachePags(listaPags : ref tCachePags)
{
	i: int;

	#Inicializamos el indice
	listaPags.sig= 0;

	#Instanciamos el array...¡aqui se le da tamaño!
	listaPags.pag = array [MAX_PAGS] of tPag;
	
	#y el resto de la estructura
	for(i=0; i<MAX_PAGS; i++)
	{
		listaPags.pag[i].ruta = nil;
		listaPags.pag[i].contenido = nil;
	}
}
##########################################################


#Muestra por pantalla el contenido de la cache
##########################################################
pintartCachePags(listaPags : ref tCachePags)
{
	i: int;

	sys->print("Nº total de paginas en cache : %s \n", string (listaPags.sig));

	for(i=0; i<listaPags.sig; i++)
	{
		sys->print("--------------- Pagina %s---------\n", string (i + 1));
		sys->print("Ruta : %s \n", listaPags.pag[i].ruta);
		sys->print("Contenido : %s \n", listaPags.pag[i].contenido);
		sys->print("---------------------------------\n");
	}
}
##########################################################


#Busca en cache la pagina solicitada. Devuelve la pos donde esta, -1 si no.
##########################################################
estaEnCache(listaPags : ref tCachePags, ruta : string) : int
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


#Guarda la informacion de un nuevo nodo
############################################################
insertarPag(listaPags : ref tCachePags, ruta : string, contenido : string) : int
{
	llena: int;

	#Comprobamos que aun queda espacio
	if (listaPags.sig == MAX_PAGS) 
	{
		llena = -1;
	}else{
		#Guardamos la info
		listaPags.pag[listaPags.sig].ruta = ruta;
		listaPags.pag[listaPags.sig].contenido = contenido;
		#Actualizamos indice y valor de retorno
		listaPags.sig++ ;
		llena = 1;
	}
	
	return llena;
}
############################################################
	
#Lee el contenido de un fichero. Devuelve el contenido para el get, y ademas
#lo guarda en la cache si es la primera vez que lo piden
############################################################
servirGet(listaPags : ref tCachePags, ruta : string) : (string, int)
{
	contenido : string;
	nBytesLeidos : int;
	buffer := array[MAX_BYTES] of byte;
	desc_fichero : ref FD;
	enCache : int;
	error, error2 : int;

	
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
			sys->print("la pagina solicitada ya estaba en cache!\n");
			contenido= listaPags.pag[enCache].contenido;
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
			error2= insertarPag(listaPags, ruta, contenido);
			
			if (error2<0) 
			{
				sys->print("no fue posible, la cache esta llena \n");
			}else{
				sys->print("ok \n");
			}	
			
			
			
		}#Cache/no cache
	}#Existe/no_existe

	return (contenido,error);
}
###################################################################



#Modifica el contenido de la pagina, tanto en el fichero como en la cache. 
###################################################################
servirPut (listaPags : ref tCachePags, ruta : string, nuevoContenido :  string) : int
{
	nBytesEscritos : int;
	desc_fichero : ref FD;
	error, error2 : int;
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
				sys->print("el fichero no constaba en cache, agregando en cache...");
				error2=insertarPag(listaPags, ruta, nuevoContenido);
				if (error2<0)
				{
					sys->print("cache llena!. El contenido sera actualizado solo en el fichero");
				}else{
					sys->print("ok");
				}

			}else{
				sys->print("el fichero ya constaba en cache, actualizando su contenido...\n");
				listaPags.pag[enCache].contenido= nuevoContenido;
			
			}#cache/no_cache

		}#existe/no_existe
	}
	
	return error;
}
				
		


#programa principal
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
		sys->print("\n######## Inicializacion del server ###########\n");
              	sys->print("---> Atado correctamente al puerto :  %s \n", puerto);
		sys->print("---> Max paginas en cache : %s \n", string MAX_PAGS);
		sys->print("########################################\n\n");	
        }
        #####################################################
		
        for(;;){
		{
		ccon: Sys->Connection;
		sys->print("Esperando peticiones...\n");

		#Creamos un nuevo socket para cada peticion
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

		spawn tratar_cliente(ccon.dfd, listaPags);
		}exception error2{
			* =>
				;
		}

	}
}

tratar_cliente(conex_cliente: ref FD, listaPags: ref tCachePags)
{
        	salir : int;
	buffer_peticion: array of byte;
	error: int;
	error_s: string;
	contenido : string;

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
			sys->print("Finalizando la conexion  con cliente...\n");
			sys->print("-----------------------------\n");
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
				sys -> print("Recibida peticion Exit, finalizando conexion con cliente...\n");
				salir=1;
			Get =>
				
				sys-> print("recibida peticion get de %s\n", r.name);
			
				#Llamamos a la func q lo trata, con los parametros del mensaje
				(contenido,error) = servirGet(listaPags, r.name);
	
				if (error>0) 
				{				
					#Si todo fue correcto, devolvemos el cotenido en instancia de tipoGet
					respuesta = ref Repmsg.Get(contenido);
				}else{
					#Si no, devolvemos instancia de tipoError
					respuesta = ref Repmsg.Error("no existe");
				}

			Put =>
				sys->print("Recibida peticion put de %s \n", r.name);

				#Llamamos a la func q lo trata, con los parametros del mensaje
				error = servirPut(listaPags,r.name,r.content);
					
				if(error >0)
				{
					#Si todo fue correcto, devolvemos ack en instancia tipoPut
					respuesta = ref Repmsg.Put();
				}else{
					#Si no, devolvemos instancia tError 
					respuesta =  ref Repmsg.Error("no existe");
				}				

			End =>
				sys->print("Recibida peticion tipo End, de momento no la tratamos\n");
				respuesta = ref Repmsg.End();
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

