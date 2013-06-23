# v1.0
# Utilidades para WP
#

Wp: module {
	PATH: con "/usr/drozas/aso/wp.dis";
	VERSION: con "wp1";
	TExitreq,
	TGetreq,
	TPutreq,
	TEndreq,
	TExitrep,
	TGetrep,
	TPutrep,
	TEndrep,
	TError,
	Tmax	 : con iota;

	Reqmsg: adt {
		pick {
		Exit =>
		Get =>
			name : string;
		Put =>
			name : string;
			content: string;
		End =>
			name : string;
		}
		packedsize:	fn(m: self ref Reqmsg): int;
		pack:	fn(m: self ref Reqmsg) : array of byte;
		unpack:	fn(buf: array of byte): (ref Reqmsg, string);
		text:	fn(m: self ref Reqmsg): string;
	};

	Repmsg: adt {
		pick {
		Exit =>
		Get =>
			content: string;
		Put =>
		End =>
		Error =>
			cause: string;
		}
		packedsize:	fn(m: self ref Repmsg): int;
		pack:	fn(m: self ref Repmsg) : array of byte;
		unpack:	fn(buf: array of byte): (ref Repmsg, string);
		text:	fn(m: self ref Repmsg) : string;
	};

	readmsg: fn(fd: ref Sys->FD, maxsize: int) : (array of byte, string);
	writemsg: fn(fd: ref Sys->FD, m: array of byte) : int;

	setup: fn();
};
