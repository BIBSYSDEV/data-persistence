from classes.RequestHandler import RequestHandler

MISSING_EVENT = 'Missing event'


def handler(event, context):
    if event is None:
        raise ValueError(MISSING_EVENT)
    else:
        request_handler = RequestHandler()
        return request_handler.handler(event, context)
