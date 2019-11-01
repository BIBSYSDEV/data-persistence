# data-persistence

Performs CRUD operations in DynamoDB

### Testing using local sam-dynamodb-local

Follow steps to deploy application at: <https://github.com/ganshan/sam-dynamodb-local>

TLDR:

    docker run -p 8000:8000 amazon/dynamodb-local
    aws dynamodb create-table --cli-input-json file://test/create-local-table.json --endpoint-url http://localhost:8000
    sam build
    sam local start-api --env-vars test/sam-local-envs.json

You should now be able to insert a resource:
    
    curl -v -X POST 'http://127.0.0.1:3000/' -d '{"operation": "INSERT","resource": {"metadata": {"titles": {"no": "En tittel","en": "A title"}}}}'