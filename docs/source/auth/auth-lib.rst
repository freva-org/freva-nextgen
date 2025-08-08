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

You can create a token using your username and password, but for security reasons
your password cannot be passed as a command-line argument.
Currently, a new token can only be created *from scratch* via the website of your host
instance.
Once logged in, click the **Token button** to generate a token, then copy, paste,
or download it.

Store your token securely in a file and use it as refresh token to create new tokens:

.. code:: console

    freva-client auth --token-file ~/.my_old_token.json > ~/.my_new_token.json
    chmod 600 ~/.my_new_token.json

.. warning::

    Avoid storing access tokens insecurely. Access tokens are sensitive and
    should be treated like passwords. Do not store them in publicly readable
    plaintext or in code repositories. Instead:

    - Use environment variables or secure storage (e.g. ``.netrc``, OS keychains).
    - Rotate and expire tokens regularly if implementing long-running SPs.
