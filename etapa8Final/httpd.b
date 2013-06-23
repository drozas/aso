#--
#-- Fichero: httpd.b
#-- Autor: Andres L. Martinez portado a ed 4 por G.
#-- Fecha: Oct 2003 modificado por G 27 Sept.
#-- Descripcion: Proxi HTTP/WP 
#-- NOTA: Usage: %s web_srv_addr wp_srv_addr
#--
implement Httpd;

include "sys.m";
include "draw.m";
include "wp.m";

sys: Sys;
wp: Wp;
Reqmsg, Repmsg, readmsg, writemsg : import wp;


Httpd: module
{
	PATH: con "./httpd.dis";
	Max_Page_Size: con 1024*1024;
	init: fn(cntx: ref Draw->Context, argv: list of string);
};


init(cntx: ref Draw->Context, argv: list of string)
{
	sys = load Sys Sys->PATH;
	wp = load Wp Wp->PATH;

	wp->setup();

	if (len argv != 3){
		sys->print("Usage: %s web_srv_addr wp_srv_addr\n",hd argv);
		raise "fail: usage";
	}


	argv = tl argv;
	web_srv_addr := hd argv;
	argv = tl argv;
	wp_srv_addr   := hd argv;

	(Err, HttpdSrv) := sys->announce(web_srv_addr);
	if (Err < 0){
		sys->print("Can't announce %s: %r\n",web_srv_addr);
		raise "fail: can't announce";
	}

	for(;;){
		
		(Err, Client) := sys->listen(HttpdSrv);
		if (Err < 0){
			sys->print("Can't listen: %r\n");
			raise "fail: can't listen";
		}
		{
		Client.dfd = sys->open(Client.dir + "/data", sys->ORDWR);
		if (Client.dfd == nil){
			sys->print("Can't open connection %s\n",Client.dir);
			raise "fail: Can't open connection";
		}
		} exception e{
		"fail:*" =>
			sys->print("Opss, fatal error on %s processing: %s",Client.dir,e);
		}
		spawn serve(wp_srv_addr,Client.dfd);
	}
}

serve(wp_srv_addr: string, Client: ref sys->FD)
{

	Wfd:=connect(wp_srv_addr);
	if (Wfd== nil){
		sys->print("Can't connect with %s: %r\n",wp_srv_addr);
		raise "fail: connect";
	}

         buff:= array[Max_Page_Size] of byte;
	response:=array of byte "HTTP/1.1 200 OK\r\n\r\n";
 	
	dataReaded:=sys->read(Client,buff,Max_Page_Size);
	if (dataReaded<0){
            	sys->print("Error reading client request: %r\n");
		raise "fail: read";
	}
         
	if (sys->write(Client,response,len response)!=len response){
		sys->print("Error writing client response:%r\n");
		raise "fail: write";
	}
        
	if (dataReaded > 3){

		msg_p     :ref Reqmsg;
		msg_r     :ref Repmsg;
		buf           : array of byte;
		szError    : string;
		wc           : int;

		case string buff[0:3] {
			"GET" =>
				(e,tlist):=sys->tokenize(string buff[0:dataReaded]," ");
				if (tlist!=nil){
					msg_p = ref Reqmsg.Get(hd tl tlist);
					buf  = msg_p.pack();

					wc  = writemsg(Wfd, buf);
					if (wc != len buf){
						sys->print("Error writining message to wp srv\n");
						raise "fail: Error writining message to wp srv";
					}

					(buf,szError)=readmsg(Wfd,Max_Page_Size);
					if (szError!=nil){
						sys->print("Error reading message: %s\n", szError);
						raise "fail: Error reading message: " + szError;
					}

					(msg_r, szError) = Repmsg.unpack(buf);
					if (szError != nil){
						sys->print("Invalid msg: %s\n", szError);
						raise "fail: invalid msg: " + szError;
					}

					pick r:= msg_r {
						Get =>
							buf = array of byte r.content;
						Put =>
						End =>
						Error =>
							sys->print("Error on wp protocol\n");
							raise "fail: error on wp protocol";
					}
					if (sys->write(Client,buf, len buf)!=len buf){
						sys->print("Error writing to client: %r\n");
						raise "fail: error writing to client";
					}
				}else{
					sys->print("->ERROR: %s\n", string buff[0:dataReaded]);
					raise "fail: invalid http request";
				}
			"PUT" =>

				hFind:=0;
				hLen:=0;
				
				while ((!hFind) && (hLen<= dataReaded)){
					if ('\r'== int buff[hLen]){
						hLen++;
						if (hLen<= dataReaded){
							if ('\n'== int buff[hLen]){
								hLen++;
								if (hLen<= dataReaded){
									if ('\r'== int buff[hLen]){
										hLen++;
										if (hLen<= dataReaded){
											if ('\n'== int buff[hLen])
												hFind=1;
												
										}
									}
								}

							}
						}
					}
					hLen++;	
				}

				if (hFind){
					
					(e,tlist):=sys->tokenize(string buff[0:hLen]," ");
					if (tlist!=nil){
						name:=hd tl tlist;
						content:=string buff[hLen:dataReaded];
						
						msg_p = ref Reqmsg.Put(name,content);
						buf  = msg_p.pack();
						wc  = writemsg(Wfd, buf);
						if (wc != len buf){
							sys->print("Error writining message to wp srv\n");
							raise "fail: Error writining message to wp srv";
						}

						(buf,szError)=readmsg(Wfd,Max_Page_Size);
						if (szError!=nil){
							sys->print("Error reading message: %s\n", szError);
							raise "fail: Error reading message: " + szError;
						}

						(msg_r, szError) = Repmsg.unpack(buf);
						if (szError != nil){
							sys->print("Invalid msg: %s\n", szError);
							raise "fail: invalid msg: " + szError;
						}
						pick r:= msg_r {
							Put =>
								sys->print("Ok");

							Error =>
								sys->print("Error on wp protocol: %s\n",r.cause);
								raise "fail: error on wp protocol";

							Get =>
							End =>
							* =>
								sys->print("Error on wp protocol\n");
								raise "fail: error on wp protocol";
						}
			
					}else {
					 	sys->print("->PUT Error: %s\n",string buff[0:dataReaded]);
						raise "fail: invalid http request";
					}			
				}
				else{
					sys->print("->PUT Error: %s\n",string buff[0:dataReaded]);
					raise "fail: invalid http request";
				}
				
			* =>
				sys->print("->ERROR: %s\n",string buff[0:dataReaded]);
				raise "fail: invalid http request";
		}
	}
	else{
		sys->print("->ERROR: %s\n", string buff[0:dataReaded]);
		raise "fail: invalid http request";
	}

}

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

connect(dest: string): ref sys->FD
{
	# Code borrowed from /appl/cmd/mount.b

	(n, nil) := sys->tokenize(dest, "!");
	if(n == 1){
		fd := sys->open(dest, sys->ORDWR);
		if(fd != nil)
			return fd;
		if(dest[0] == '/') 
			return nil;
	}
	(ok, c) := sys->dial(netmkaddr(dest, "tcp", nil), nil);
	if(ok < 0)
		return nil;
	return c.dfd;
}
