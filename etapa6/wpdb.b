# v1.0
# Depurador de WP
# Sirve para imprimir trazas de mensajes de WP
#
# limbo -g wpdb.b
implement Wpdb;

Wpdb: module
{
	PATH	: con "./wpdb.dis";

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

argv0: string;

usage()
{
	fprint(sys->fildes(2), "usage: %s direccionsrv direccionmia\n", argv0);
	raise "fail:usage";
}

init(nil: ref Draw->Context, argv: list of string)
{
	argv0 = hd argv;
	argv  = tl argv;

	sys = load Sys Sys->PATH;
	wp = load Wp Wp->PATH;
	wp->setup();

	if (len argv != 2)
		usage();
	srvaddr := hd argv; argv = tl argv;
	addr    := hd argv; argv = tl argv;

	fds := connect(srvaddr);
	if (fds == nil)
		raise sprint("fail: can't connect: %r");
	fdc := acceptcli(addr);
	for(;;){
		reqdb(fdc, fds, fildes(2));
		repdb(fds, fdc, fildes(2));
	}
}

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

kill(pid : int)
{
	f := open("/prog/" + string pid + "/ctl", OWRITE);
	if (f == nil)
		return;
	write(f, array of byte "kill", 4);
}


reqdb(in: ref FD, out: ref FD, dbg: ref FD)
{
	(buf, e) := readmsg(in, 10*1024);
	if (e != nil){
		fprint(dbg, "req: readmsg: %s", e);
		exit;
	}
	(m, es) := Reqmsg.unpack(buf);
	if (es != nil){
		fprint(dbg, "req: readmsg: %s", es);
		exit;
	}
	fprint(dbg, "cli→srv: %s\n", m.text());
	wl := write(out, buf, len buf);
	if (wl != len buf){
		fprint(dbg, "req: can't write msg: %r\n");
		exit;
	}
}

repdb(in: ref FD, out: ref FD, dbg: ref FD)
{
	(buf, e) := readmsg(in, 10*1024);
	if (e != nil){
		fprint(dbg, "rep: readmsg: %s", e);
		exit;
	}
	(m, es) := Repmsg.unpack(buf);
	if (es != nil){
		fprint(dbg, "rep: readmsg: %s", es);
		exit;
	}
	fprint(dbg, "srv→cli: %s\n", m.text());
	wl := write(out, buf, len buf);
	if (wl != len buf){
		fprint(dbg, "rep: can't write msg: %r\n");
		exit;
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
