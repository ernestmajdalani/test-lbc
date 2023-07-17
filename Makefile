build :
	rm -rf ./dist && mkdir ./dist
	cp ./main.py ./dist
	cp ./config.json ./dist
	cp -r data ./dist/data
	zip -r dist/jobs.zip jobs
	zip -r dist/utils.zip utils