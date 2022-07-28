
Code status:
------------

## DLMS (Distributed Ledger Management System)

DLMS is an envrionment for deploy and use Distriuted Ledgers on TrustSQL.
It consists of TOSS(Trusted Ordering-Stamping Server) , DNOM (Distributed Node Manager) and and demonstrations.

## TOSS 
TOSS is single point of receipt for all user tranactions.
It works as a total ordering broadcaster and also message server.
Trusted means it includes singnature stamp of trusted authorigy.

## DNOM 
DNOM is an agent to subscribe messages from TOSS and insert it into TrustSQL.

## demonstrations.
You can find how it works and what can you do with it.


Build:
--------

#### build path.
If you download DLMS at dlms/dlms_work
export RDLMSPATH=$HOME/dlms/rdlms_work
export RDLMSBIN=$RDLMSPATH/bin
export PATH=$PATH:$RDLMSBIN

#### compile
./build.sh


#### deploy.
./deploy.sh


Run:
--------

1. DNOM
dnom_start.sh [config_file]

2. TOSS
toss_start.sh [config_file]



Who we are:
----------
TrustDB inc, is a distributed ledger technology company in South, Korea.
 

Help:
-----
If you need any help please send me an e-mail.
booltaking@gmail.com


License:
--------

***************************************************************************

NOTE: 

TrustSQL is specifically available only under version 2 of the GNU
General Public License (GPLv2). (I.e. Without the "any later version"
clause.) This is inherited from MariaDB.

***************************************************************************
