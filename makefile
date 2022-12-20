
db:
	docker run --rm -e \
	POSTGRES_PASSWORD=test \
	-p=5432:5432 \
	--volume=$(PWD)/pg.conf:/etc/postgresql.conf \
	postgres:15 postgres -c config_file=/etc/postgresql.conf
