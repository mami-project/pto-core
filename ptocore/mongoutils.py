from pymongo.collection import Collection, ReturnDocument

class UnknownField(Exception):
    pass

class AutoIncrementFactory:
    def __init__(self, coll: Collection):
        self.coll = coll

    def create(self, field: str):
        # separate from incrementor because it's not atomic
        self.coll.insert_one({'_id': field, 'next': 0})

    def delete(self, field: str):
        self.coll.delete_one({'_id': field})

    def get_incrementor(self, field: str, create_if_missing=False):
        """
        Returns a closure which increments the value of the given field. The increment operation is globally atomic.
        :param field: The name of the field for which an incrementor function should be returned.
        :param create_if_missing: Do not raise error if field is not existing. Automatically create it.
                                  Note that checking and creating the field is not atomic.
        :raises UnknownField: if the field does not exist.
        :return: A closure which returns the next value and increments the field in the database atomically.
        """

        # check if field exists
        if self.coll.find_one({'_id': field}) is None:
            if create_if_missing is False:
                raise UnknownField()
            else:
                self.create(field)

        # create incrementor function and return it to the caller
        def func():
            doc = self.coll.find_one_and_update(
                {'_id': field},
                {'$inc': {'next': 1}},
                return_document=ReturnDocument.BEFORE
            )
            if doc is None:
                raise UnknownField()

            # note that this is the value before incrementing it.
            return doc['next']

        return func

class TypelockFactory:
    def __init__(self, coll: Collection):
        self.coll = coll

    def create(self, field: str):
        self.coll.update_one(
            {'_id': field},
            {'_id': field, 'locked': False, 'owner': None},
            upsert=True)

    def delete(self, field: str):
        self.coll.delete_one({'_id': field})

    def get_locker(self, field: str, owner: str):
        # check if field exists
        if self.coll.find_one({'_id': field}) is None:
            raise UnknownField()

        # create lock/unlock function and return it to the caller
        def func(lock: bool):
            doc = self.coll.find_one_and_update(
                {'_id': field,
                 '$or': [
                     {'locked': False},
                     {'locked': True, 'owner': owner}
                 ]},
                {'locked': True, 'owner': owner}
            )

            if doc is not None:
                # we got the lock
                return True
            else:
                # lock failed.
                # now determine if it does even exist?
                doc = self.coll.find_one({'_id': field})
                if doc is None:
                    raise UnknownField()
                else:
                    # lock exists and we were not able to acquire it.
                    return False

        return func

