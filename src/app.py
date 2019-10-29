from classes.RequestHandler import RequestHandler


def handler(event, context):
    if event is None:
        raise ValueError("missing event")
    else:
        request_handler = RequestHandler()
        return request_handler.handler(event, context)
