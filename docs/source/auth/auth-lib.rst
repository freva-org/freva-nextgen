.. _auth_lib:

Authentication using the freva-client library
=============================================

The freva-client python library offers a very simple interface to interact
with the authentication system.

.. autofunction:: freva_client.authenticate

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

You can create a token using your user name and password.

In the process of token generation, you would want to store your token data *securely*
in a file, and use it as a refresh token to create new ones, eventually:

.. code:: console

    freva-client auth  > ~/.mytoken.json
    chmod 600  ~/.mytoken.json


For security reasons you cannot pass your password as an argument to the command line
interface. This means that, in a *non-interactive* session such as a batch job, you
will have two options:

1. Either use the valid token with ``--token-file <my_token_file>``.
2. Or, if you want to create a new one, you will *only* be able to do it with help
   of an already pre-existing valid refresh token.

   .. code:: console

       freva-client auth --token-file ~/.my_old_token.json > ~/.my_new_token.json
       chmod 600 ~/.my_new_token.json


.. warning::

    Avoid storing access tokens insecurely. Access tokens are sensitive and
    should be treated like passwords. Do not store them in publicly readable
    plaintext or in code repositories. Instead:

    - Use environment variables or secure storage (e.g. ``.netrc``, OS keychains).
    - Rotate and expire tokens regularly if implementing long-running SPs.
