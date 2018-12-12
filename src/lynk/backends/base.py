class BaseBackend(object):
    def put(self, item, condition=None):
        raise NotImplementedError('put')

    def update(self, key, updates, condiiton=None):
        raise NotImplementedError('update')

    def delete(self, key, condition=None):
        raise NotImplementedError('delete')

    def get(self, key, attributes):
        raise NotImplementedError('get')
