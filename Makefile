

run:
	BXLOGIC_HOME=`pwd` PYTHONPATH=`pwd` python bxlistener.py --configfile config/bxcfg.yaml

qlisten:
	BXLOGIC_HOME=`pwd` PYTHONPATH=`pwd` ./sqs-consume.py --config config/queue_consumer.yaml --source bxlogic --verbose

regen:
	BXLOGIC_HOME=`pwd` PYTHONPATH=`pwd` routegen -e config/bxcfg.yaml > bxlistener.py

init_db:
	cat sql/bxlogic_ddl.sql | pgexec --target bxlogic_db --db binary_test -s
	cat sql/bxlogic_initial_data.sql | pgexec --target bxlogic_db --db binary_test -s