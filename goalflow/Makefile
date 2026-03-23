.PHONY: up down logs test clean report

up:
	docker-compose up --build -d

down:
	docker-compose down

logs:
	docker-compose logs -f

test:
	pytest tests/ -v

clean:
	docker-compose down -v
	rm -rf logs/

report:
	docker-compose exec analytics python report.py
