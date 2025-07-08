.. _auth_lib:

Authentication using the freva-client library
=============================================

The freva-client python library offers a very simple interface to interact
with the authentication system.

.. automodule:: freva_client
   :members: authenticate

.. _auth_cli:

Using the command line interface
================================

Token creation and refreshing can also be achieved with help of the ``auth``
sub command of the command line interface

.. code:: console

    freva-client auth --help

.. execute_code::
   :hide_code:

   from subprocess import run, PIPE
   res = run(["freva-client", "auth", "--help"], check=True, stdout=PIPE, stderr=PIPE)
   print(res.stdout.decode())

You can create a token using your user name and password. For security reasons
you can not pass your password as an argument to the command line interface.
This means that you can only create a new token with help of a valid refresh
token in a non-interactive session. Such as a batch job.

Therefore you want to store your token data securely in a file, and use the
refresh token to create new tokens:

.. code:: console

    freva-client auth  > ~/.mytoken.json
    chmod 600 ~/.mytoken.json

Later you can use the `jq json command line parser <https://jqlang.github.io/jq/>`_
to read the refresh token from and use it to create new access tokens.

.. code:: console

    freva-client auth --token-file ~/.mytoken.json > ~/.mytoken.json


.. warning::

    Avoid storing access tokens insecurely. Access tokens are sensitive and
    should be treated like passwords. Do not store them in publicly readable
    plaintext or in code repositories. Instead:

    - Use environment variables or secure storage (e.g. ``.netrc``, OS keychains).
    - Rotate and expire tokens regularly if implementing long-running SPs.
