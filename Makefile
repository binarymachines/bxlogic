

run:
	BXLOGIC_HOME=`pwd` PYTHONPATH=`pwd` python bxlistener.py --configfile config/bx_web.yaml

qlisten:
	BXLOGIC_HOME=`pwd` PYTHONPATH=`pwd` ./sqs-consume.py --config config/bx_sqs.yaml --source bxlogic --verbose

regen:
	BXLOGIC_HOME=`pwd` PYTHONPATH=`pwd` routegen -e config/bx_web.yaml > bxlistener.py

init_db:
	cat sql/bxlogic_ddl.sql | pgexec --target bxlogic_db --db binary_test -s
	cat sql/bxlogic_initial_data.sql | pgexec --target bxlogic_db --db binary_test -s