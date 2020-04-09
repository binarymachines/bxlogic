

run:
	BXLOGIC_HOME=`pwd` PYTHONPATH=`pwd` python bxlistener.py --configfile config/bx_web.yaml

qlisten:
	BXLOGIC_HOME=`pwd` PYTHONPATH=`pwd` ./sqs-consume.py --config config/bx_sqs.yaml --source bxlogic

qscan:
	BXLOGIC_HOME=`pwd` PYTHONPATH=`pwd` ./sqs-consume.py --config config/bx_sqs.yaml --source bxlogic-scan

qsend_arbitrate:
	./sqssend.py --url https://sqs.us-east-1.amazonaws.com/543680801712/bxlogic_events --body 'arbitration event' --attrs=eventtype:arbitration%String

regen:
	BXLOGIC_HOME=`pwd` PYTHONPATH=`pwd` routegen -e config/bx_web.yaml > bxlistener.py

init_db:
	cat sql/bxlogic_ddl.sql | pgexec --target bxlogic_db --db binary_test -s
	cat sql/bxlogic_initial_data.sql | pgexec --target bxlogic_db --db binary_test -s