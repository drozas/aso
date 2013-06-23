# v1.0
# Shell para hablar con un servidor WP
#

implement Wpsh;


include "sys.m";
	sys: Sys;
	fprint, fildes, FD, ORDWR, tokenize, open, dial, pctl, sprint,
	announce, listen, OWRITE, write, print : import sys;
include "draw.m";

include "wp.m";
	wp: Wp;
	Reqmsg, Repmsg, readmsg, writemsg : import wp;

include "bufio.m";
	bufio: Bufio;
	Iobuf: import bufio;

Wpsh: module
{
	PATH	: con "/usr/drozas/aso/wpsh.dis";

	init	: fn(nil: ref Draw->Context, argv: list of string);
};

argv0: string;

usage()
{
	fprint(fildes(2), "usage: %s direccion\n", argv0);
	raise "fail: usage";
}

init(nil: ref Draw->Context, argv: list of string)
{
	argv0 = hd argv;
	argv  = tl argv;

	sys = load Sys Sys->PATH;
	wp = load Wp Wp->PATH;
	bufio = load Bufio Bufio->PATH;
	wp->setup();
	if (len argv != 1)
		usage();
	addr := hd argv; argv = tl argv;

	fds:= connect(addr);
	if (fds == nil)
		raise sprint("fail: can't connect: %r");
	in := bufio->fopen(fildes(0), bufio->OREAD);
	for(;;){
		print("=> ");
		ln := in.gets('\n');
		if (ln == nil)
			break;
		parse(ln, fds);
	}
}

parse(ln : string, fd : ref FD)
{
	(rc, wl) := tokenize(ln, " \t\n");
	if (rc <= 0){
		print("mala linea\n");
		return;
	}
	c := hd wl;
	wl = tl wl;
	msg : ref Reqmsg;
	case c {
	"exit" =>
		if (len wl != 0){
			print("uso: exit\n");
			return;
		}
		msg = ref Reqmsg.Exit();
	"get" =>
		if (len wl != 1){
			print("uso: get nombre\n");
			return;
		}
		name := hd wl;
		msg = ref Reqmsg.Get(name);
	"put" =>
		if (len wl != 2){
			print("uso: put nombre string\n");
			return;
		}
		name := hd wl; wl = tl wl;
		content := hd wl;
		msg = ref Reqmsg.Put(name, content);
	"end" =>
		if (len wl != 1){
			print("uso: end nombre\n");
			return;
		}
		name := hd wl;
		msg = ref Reqmsg.End(name);
	* =>
		print("el comando no existe. (ha de ser: exit get put end)\n");
		return;
	}
	buf := msg.pack();
	wc := writemsg(fd, buf);
	if (wc != len buf)
		raise "fail: error al escribir el mensaje";
	#print("peticion: %s\n", msg.text());
	#print("respuesta:");
	(rbuf, s) := readmsg(fd, 10*1024);
	if (s != nil){
		print("%s\n", s);
		return;
	}
	(rmsg, rs) := Repmsg.unpack(rbuf);
	if (rs != nil){
		print("%s\n", rs);
		return;
	}
	#print("%s\n", rmsg.text());
	pick r := rmsg {
	Put =>
	Get =>
		print("%s\n", r.content);
	End =>
	Error =>
		print("error: %s\n", r.cause);
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

connect(dest: string): ref FD
{
	# Code borrowed from /appl/cmd/mount.b

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
