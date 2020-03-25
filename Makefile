

listen:
	BXLOGIC_HOME=`pwd` PYTHONPATH=`pwd` python bxlistener.py --configfile config/bxcfg.yaml


init_db:
	cat sql/bxlogic_ddl.sql | pgexec --target bxlogic_db --db binary_test -s