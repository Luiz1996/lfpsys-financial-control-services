This is the Financial Control Service of LFPSys.

#### Main Technology Stack
* Java 21
* PostgreSQL
* Apache Kafka
* Redis
* Maven 3.9.1 (or higher)
* [Spring Boot Framework](https://spring.io/projects/spring-boot)

#### Communication Contracts
##### KAFKA
	Tópico: financial-control
	Headers:
		client_id: <UUID>
	Payload:
		{
			"payment": [
				{
					"nfeId": <UUID>,
					"operation": <Enum>, //ACCOUNTS_PAYABLE ou ACCOUNTS_RECEIVABLE
					"value": "100.55",
					"clientOrSupplierId": <UUID>
				}
			]
		}