# Colombo libunftp patch

This directory contains the crates.io `libunftp` 0.23.0 package so Colombo can
carry one narrowly scoped compatibility correction while upstream issue #439
remains open.

Colombo changes `setup_inter_loop_comms` to abort an already accepted passive
data socket before a successive `PASV` or `EPSV` command installs replacement
channels. Colombo also enables libunftp's pooled listener mode, which retires a
reserved-but-unconnected passive endpoint when the client requests another one.
Together these restore Apache FtpServer v1's close-before-replace behavior for
persistent camera clients.

Colombo also snapshots restart state before executing a data command instead
of holding the shared FTP session mutex for the duration of the transfer. This
keeps the pooled control listener responsive when a camera abandons its control
connection but leaves its data socket open.

The regression is exercised by `tests/ftp_pasv_state.py` through the public FTP
protocol boundary, including an abandoned `STOR` followed by a new control
connection. Remove the vendored patch and the `[patch.crates-io]` entry only
after an upstream release closes the prior accepted data socket, never holds a
session mutex across transfer I/O, and passes that test unchanged.
