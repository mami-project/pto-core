from pymongo.collection import Collection, ReturnDocument

class AutoIncrementUnknownField(Exception):
    pass

class AutoIncrementFactory:
    def __init__(self, coll: Collection):
        self.coll = coll

    def create(self, field: str):
        # separate from incrementor because it's not atomic
        self.coll.insert_one({'_id': field, 'next': 0})

    def delete(self, field: str):
        self.coll.delete_one({'_id': field})

    def get_incrementor(self, field: str):
        # check if field exists
        if self.coll.find_one({'_id': field}) is None:
            raise AutoIncrementUnknownField()

        # create incrementor function and return it to the caller
        def func():
            doc = self.coll.find_one_and_update(
                {'_id': field},
                {'$inc': {'next': 1}},
                return_document=ReturnDocument.BEFORE
            )
            if doc is None:
                raise AutoIncrementUnknownField()

            # note that this is the value before incrementing it.
            return doc['next']

        return func
