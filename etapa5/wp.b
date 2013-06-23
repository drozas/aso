# v1.1
# Utilidades para WP
#

implement Wp;

include "sys.m";
	sys: Sys;

include "wp.m";

Strmagic: con 32;
Msgmagic: con "dead";

setup()
{
	sys = load Sys Sys->PATH;
}

packedstrsize(s: string) : int
{
	if (s == nil)
		s = "";
	sbytes := array of byte s;	# could do conversion ourselves
	return 1 + 2 + len sbytes;
}

packstr(buf: array of byte, pos: int, s: string) : int
{
	if (s == nil)
		s = "";
	sbytes := array of byte s;	# could do conversion ourselves
	n := len sbytes;
	buf[pos+0] = byte Strmagic;
	buf[pos+1] = byte n;
	buf[pos+2] = byte (n>>8);
	buf[pos+3:] = sbytes;
	return pos + 3 + n;
}

unpackstr(buf: array of byte, pos: int): (string, int)
{
	if(pos < 0 || pos + 1+2 > len buf)
		return (nil, -1);
	if(buf[pos+0] != byte Strmagic)
		return (nil, -1);
	sz := (int buf[pos+2] << 8) | int buf[pos+1] ;
	send := pos + 3 + sz;
	if(send > len buf)
		return (nil, -1);
	return (string buf[pos+3:send], send);
}

Msghdrsize : con 4 + 4 + 1;

Reqmsg.packedsize(mp: self ref Reqmsg): int
{
	pick m := mp {
	Exit =>
		return Msghdrsize;
	Get =>
		return Msghdrsize + packedstrsize(m.name);
	Put =>
		return Msghdrsize + packedstrsize(m.name) + packedstrsize(m.content);
	End =>
		return Msghdrsize + packedstrsize(m.name);
	}
	return 0;
}

Repmsg.packedsize(mp: self ref Repmsg): int
{
	pick m := mp {
	Exit =>
		return Msghdrsize;
	Get =>
		return Msghdrsize + packedstrsize(m.content);
	Put =>
		return Msghdrsize;
	End =>
		return Msghdrsize;
	Error =>
		return Msghdrsize + packedstrsize(m.cause);
	}
	return 0;
}

Reqmsg.pack(mp: self ref Reqmsg) : array of byte
{
	if (mp == nil)
		return nil;
	l := mp.packedsize();
	buf := array[l] of byte;
	if (l < Msghdrsize)
		return nil;
	buf[0:] = array of byte Msgmagic;
	buf[4] = byte (l >> 24);
	buf[5] = byte (l >> 16);
	buf[6] = byte (l >> 8);
	buf[7] = byte l;
	pick m := mp {
	Exit =>
		buf[8] = byte TExitreq;
	Get =>
		buf[8] = byte TGetreq;
		packstr(buf, 9, m.name);
	Put =>
		buf[8] = byte TPutreq;
		pos := packstr(buf, 9, m.name);
		packstr(buf, pos, m.content);
	End =>
		buf[8] = byte TEndreq;
		packstr(buf, 9, m.name);
	}
	return buf;
}

Reqmsg.unpack(buf: array of byte): (ref Reqmsg, string)
{
	if (len buf < Msghdrsize)
		return (nil, "mensaje muy chico");
	t := buf[8];
	case int t {
	TExitreq =>
		m := ref Reqmsg.Exit();
		return (m, nil);
	TGetreq =>
		(s, rc) := unpackstr(buf, 9);
		if (rc < 0)
			return (nil, "error al extraer nombre");
		m := ref Reqmsg.Get(s);
		return (m, nil);
	TPutreq =>
		(s, pos) := unpackstr(buf, 9);
		if (pos < 0)
			return (nil, "error al extraer nombre");
		(c, rc) := unpackstr(buf, pos);
		if (rc < 0)
			return (nil, "error al extraer contenido");
		m := ref Reqmsg.Put(s, c);
		return (m, nil);
	TEndreq =>
		(s, rc) := unpackstr(buf, 9);
		if (rc < 0)
			return (nil, "error al extraer nombre");
		m := ref Reqmsg.End(s);
		return (m, nil);
	* =>
		return (nil, "mal tipo de mensaje");
	}
}

Repmsg.pack(mp: self ref Repmsg) : array of byte
{
	if (mp == nil)
		return nil;
	l := mp.packedsize();
	buf := array[l] of byte;

	buf[0:] = array of byte Msgmagic;
	buf[4] = byte (l >> 24);
	buf[5] = byte (l >> 16);
	buf[6] = byte (l >> 8);
	buf[7] = byte l;
	pick m := mp {
	Exit =>
		buf[8] = byte TExitrep;
	Get =>
		buf[8] = byte TGetrep;
		packstr(buf, 9, m.content);
	Put =>
		buf[8] = byte TPutrep;
	End =>
		buf[8] = byte TEndrep;
	Error =>
		buf[8] = byte TError;
		packstr(buf, 9, m.cause);
	}
	return buf;
}

Repmsg.unpack(buf: array of byte): (ref Repmsg, string)
{
	if (len buf < Msghdrsize)
		return (nil, "mensaje muy chico");
	t := buf[8];
	case int t {
	TExitrep =>
		m := ref Repmsg.Exit();
		return (m, nil);
	TGetrep =>
		(c, rc) := unpackstr(buf, 9);
		if (rc < 0)
			return (nil, "error al extraer nombre");
		m := ref Repmsg.Get(c);
		return (m, nil);
	TPutrep =>
		m := ref Repmsg.Put();
		return (m, nil);
	TEndrep =>
		m := ref Repmsg.End();
		return (m, nil);
	TError =>
		(e, rc) := unpackstr(buf, 9);
		if (rc < 0)
			return (nil, "error al extraer nombre");
		m := ref Repmsg.Error(e);
		return (m, nil);
	* =>
		return (nil, "mal tipo de mensaje");
	}
}

Reqmsg.text(mp: self ref Reqmsg) : string
{
	if (mp == nil)
		return "nil";
	pick m := mp {
	Exit =>
		return sys->sprint("EXIT REQ");
	Get =>
		return sys->sprint("GET REQ\t%s", m.name);
	Put =>
		l := len m.content;
		if (l > 20)
			l = 20;
		return sys->sprint("PUT REQ\t%s\t%s...", m.name, m.content[0:l]);
	End =>
		return sys->sprint("END REQ\t%s", m.name);
	}
	return "error";
}

Repmsg.text(mp: self ref Repmsg) : string
{
	if (mp == nil)
		return "nil";
	pick m := mp {
	Exit =>
		return sys->sprint("EXIT REP");
	Get =>
		l := len m.content;
		if (l > 20)
			l = 20;
		return sys->sprint("GET REP\t%s", m.content[0:l]);
	Put =>
		return sys->sprint("PUT REP");
	End =>
		return sys->sprint("END REP");
	Error =>
		return sys->sprint("ERROR\t%s", m.cause);
	}
	return "error";
}

readmsg(fd: ref Sys->FD, maxsize: int) : (array of byte, string)
{
	hdr := array[Msghdrsize] of byte;
	n := readn(fd, hdr, Msghdrsize);
	if (n != Msghdrsize)
		return (nil, "no hay cabecera");
	if (string hdr[0:4] != Msgmagic)
		return (nil, "no es un mensaje WP");
	l :=(int hdr[4] << 24) | 
		(int hdr[5] << 16) |
		(int hdr[6] << 8)  |
		(int hdr[7]) ;
	if (l < 0 || l > maxsize)
		return(nil, "mensaje muy grande");
	if (l < Msghdrsize)
		return(nil, "mensaje muy chico");
	tag := int hdr[8];
	if (tag < 0 || tag >= Tmax)
		return (nil, "tipo de mensaje invalido");
	msg := array[l] of byte;
	msg[0:] = hdr[0:Msghdrsize];
	n = readn(fd, msg[Msghdrsize:], l - Msghdrsize);
	if (n != l - Msghdrsize)
		return (nil, "mensaje truncado");
	return (msg, nil);
}

writemsg(fd: ref Sys->FD, msg: array of byte) : int
{
	return sys->write(fd, msg, len msg);
}



readn(fd: ref Sys->FD, buf: array of byte, nb: int): int
{
	for(nr := 0; nr < nb;){
		n := sys->read(fd, buf, nb-nr);
		if(n <= 0){
			if(nr == 0)
				return n;
			break;
		}
		nr += n;
	}
	return nr;
}

