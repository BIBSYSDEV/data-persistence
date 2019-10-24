from lib.RequestHandler import RequestHandler


def handler(event, context):
    request_handler = RequestHandler()
    return request_handler.handler(event, context)
