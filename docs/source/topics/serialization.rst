Serialization
=============


Overview
--------

A lock can be used to control access to some shared resource. In a serverless
environment jobs can be broken up into many pieces often being operated on by
a sequence of disparate functions. In order to hold a lock between these
separate functions there needs to be a mechanism for a lock to be transfered
between seprate processes, or passed through a queue etc.

To accomodate this use case, Locks are serializable. A lock can be taken
against a resource in one process, serialized and passed into some other
process. The target process can then derserialize the lock and continue
operating on the same resource as the first.


Serialization
-------------

Lynk Locks use a simple JSON serialization scheme, to allow them to be passed
around as plain text between processes.

To serialize a Lock use the ``.serialize()`` method.

.. code-block:: python

   import lynk

   session = lynk.get_session('lynk-quickstart')
   lock = session.create_lock('my lock')
   serialized_lock = lock.serialize()

The ``serialized_lock`` variable is now a plain UTF-8 string that can be sent
to another component of a complex system.


Deserialization
---------------

A lock object can be loaded using the ``Session`` method
``deserialize_lock()`` and passing it the ``serialzied_lock`` value from the
previous section. If it successful the new process can now start
operating on the protected resource. Otherwise it will raise the
``LockAlreadyInUseError`` to indicate that the lock was stolen between
serialization and deserialization.

..code-block:: python

   import lynk
   from lynk.exceptions import LockAlreadyInUseError

   try:
       session = lynk.get_session('lynk-quickstart')
       lock = session.deserialize_lock(serialized_lock)
       do_stuff_with_locked_resource(lock)
   except LockAlreadyInUseError as:
       print("Someone else stole the lock.")
