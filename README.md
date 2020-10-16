# plaidtools
Common utilities useful to PlaidCloud, PlaidLink, Custom Transforms, and PlaidXL.


Plaid and SAP PCM Operations in Excel

<hr>

Installation
===============

### UNIX (Linux/Mac OSX)

Just run

```
sudo pip install -e git+https://github.com/PlaidCloud/plaidtools.git#egg=plaidtools
```

or, on a local dev machine:

```
sudo pip install -e git+ssh://git@github.com/PlaidCloud/plaidtools.git#egg=plaidtools
```

If you're on an app server or dev machine, you'll want to run this from plaid's
home folder.

If you want sandboxed users to have access to it, also run:

```
sudo chown -R plaid:sandbox src/plaidtools
```

If you get an authorization or authentication error, make sure your root user
has access to the plaidtools github repository (check its ssh keys).

### Windows, from an already cloned git repo

To install this package locally on a Windows box, for development purposes, this
approach has been successful:

1. Clone to local

2. Open Anaconda command prompt as Administrator (this is equiv to sudo access)

3. Navigate to the repo

4. Enter the following command:

``` pip install -e . ```

(Note the <space> between the -e and the '.')

**NOTE:** It may also be possible to use a single command install on Windows, like you
would on UNIX, but that has been difficult for several people.

### Upgrading an existing profitagent install to use plaidtools, and to accept the profitagent -> plaidlink name change

1. Regenerate ssh keys, in the home folder of whatever user was used to install
   profit agent, by running in git-bash:
   ``` ssh-keygen -t rsa -b 4096 -N "" ```
2. Add your new id_rsa.pub to the plaid-machine-user in github (If you don't
   have access to do that, talk to Adams or Paul)
3. Go into the existing profitagent folder. Run in git-bash:
   ``` git checkout master```
   ``` git pull origin master```
   (If the second one fails, you've done something wrong with ssh keys - go back
   to the beginning)
4. Go into the folder above profitagent - probably {home folder}/src/ . Run in
   git-bash:
   ``` git clone git@github.com:PlaidCloud/plaidtools.git```
5. Go into the new plaidtools folder. Run in _the Anaconda prompt_:
   ``` pip install -e .```
6. Go back into profitagent/plaidlink . Run in git-bash:
   ``` python windows_service.py --startup auto install```
7. Go into the Windows Services gui, and restart Plaid Agent
8. If you didn't get any errors, you're done!

(Throughout this I assume that plaidlink is installed in a folder called
profitagent. These instructions don't change that name, but you can if you want,
just do it before step 6.)
