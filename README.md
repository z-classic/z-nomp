# Zcash® and Zclassic - Node Open Mining Portal

**[Click here for the official - Zcash® and Zclassic Stratum Mining Pool Installation Guide](https://zdeveloper.org/wiki:z-nomp_install)**

This is a Equihash mining pool based off of Node Open Mining Portal.

Donations for development are greatly appreciated!
  * BTC: 18vHMxVzotQ9EPyESrf7Z1hNM9AwJeVHgD
  * ZCL: zcXDWbgReztLLXSTUMT2nEumiDM6zTzUXFb7vUnx9JNfJDVqbodyxwEQwgDkFw7Dp128tBU8n8rmVxT43DshmeTEM4LHcdz

#### Production Usage Notice
This is beta software. All of the following are things that can change and break an existing Z-NOMP setup: functionality of any feature, structure of configuration files and structure of redis data. If you use this software in production then *DO NOT* pull new code straight into production usage because it can and often will break your setup and require you to tweak things like config files or redis data. *Only tagged releases are considered stable.*

#### Paid Solution
Usage of this software requires abilities with sysadmin, database admin, coin daemons, and sometimes a bit of programming. Running a production pool can literally be more work than a full-time job. 


### Community / Support
IRC
* Support / general discussion join: https://gitter.im/zclassicorg/z-nomp

If your pool uses Z-NOMP let us know and we will list your website here.

### Some pools using Z-NOMP or node-stratum-module:

https://pool.cryptobroker.io/zcl Running MPOS and 0.5% of the fee goes to the Zclassic donation fund! 200+ blocks have been found as well! 

http://luckpool.org Zcash Pool with Custom Frontend w/Miner's Jackpot

http://zclmine.com/ Custom frontend

http://zclassic.miningspeed.com Custom frontend and 0% fee

https://zpool.it 0.5% fee

http://miningpool.io/

Usage
=====


#### Requirements
* Coin daemon(s) (find the coin's repo and build latest version from source)
* [Node.js](http://nodejs.org/) v7+ ([follow these installation instructions](https://github.com/joyent/node/wiki/Installing-Node.js-via-package-manager))
* [Redis](http://redis.io/) key-value store v2.6+ ([follow these instructions](http://redis.io/topics/quickstart))

##### Seriously
Those are legitimate requirements. If you use old versions of Node.js or Redis that may come with your system package manager then you will have problems. Follow the linked instructions to get the last stable versions.


[**Redis security warning**](http://redis.io/topics/security): be sure firewall access to redis - an easy way is to
include `bind 127.0.0.1` in your `redis.conf` file. Also it's a good idea to learn about and understand software that
you are using - a good place to start with redis is [data persistence](http://redis.io/topics/persistence).


#### 0) Setting up coin daemon
Follow the build/install instructions for your coin daemon. Your coin.conf file should end up looking something like this:
```
daemon=1
rpcuser=zclassicrpc
rpcpassword=securepassword
rpcport=8232
```
For redundancy, its recommended to have at least two daemon instances running in case one drops out-of-sync or offline,
all instances will be polled for block/transaction updates and be used for submitting blocks. Creating a backup daemon
involves spawning a daemon using the `-datadir=/backup` command-line argument which creates a new daemon instance with
it's own config directory and coin.conf file. Learn about the daemon, how to use it and how it works if you want to be
a good pool operator. For starters be sure to read:
   * https://en.bitcoin.it/wiki/Running_bitcoind
   * https://en.bitcoin.it/wiki/Data_directory
   * https://en.bitcoin.it/wiki/Original_Bitcoin_client/API_Calls_list
   * https://en.bitcoin.it/wiki/Difficulty


#### 1) Downloading & Installing

Clone the repository and run `npm update` for all the dependencies to be installed:

```bash
sudo apt-get install build-essential libsodium-dev npm
sudo npm install n -g
sudo n stable
git clone https://github.com/joshuayabut/node-open-mining-portal.git z-nomp
cd z-nomp
npm update
npm install
```

##### Pool config
Take a look at the example json file inside the `pool_configs` directory. Rename it to `zclassic.json` and change the
example fields to fit your setup.

```
Please Note that: 1 Difficulty is actually 8192, 0.125 Difficulty is actually 1024.

Whenever a miner submits a share, the pool counts the difficulty and keeps adding them as the shares. 

ie: Miner 1 mines at 0.1 difficulty and finds 10 shares, the pool sees it as 1 share. Miner 2 mines at 0.5 difficulty and finds 5 shares, the pool sees it as 2.5 shares. 
```


##### [Optional, recommended] Setting up blocknotify
1. In `config.json` set the port and password for `blockNotifyListener`
2. In your daemon conf file set the `blocknotify` command to use:
```
node [path to cli.js] [coin name in config] [block hash symbol]
```
Example: inside `zclassic.conf` add the line
```
blocknotify=node /home/user/z-nomp/scripts/cli.js blocknotify zclassic %s
```

Alternatively, you can use a more efficient block notify script written in pure C. Build and usage instructions
are commented in [scripts/blocknotify.c](scripts/blocknotify.c).


#### 3) Start the portal

```bash
npm start
```

###### Optional enhancements for your awesome new mining pool server setup:
* Use something like [forever](https://github.com/nodejitsu/forever) to keep the node script running
in case the master process crashes. 
* Use something like [redis-commander](https://github.com/joeferner/redis-commander) to have a nice GUI
for exploring your redis database.
* Use something like [logrotator](http://www.thegeekstuff.com/2010/07/logrotate-examples/) to rotate log 
output from Z-NOMP.
* Use [New Relic](http://newrelic.com/) to monitor your Z-NOMP instance and server performance.


#### Upgrading Z-NOMP
When updating Z-NOMP to the latest code its important to not only `git pull` the latest from this repo, but to also update
the `node-stratum-pool` and `node-multi-hashing` modules, and any config files that may have been changed.
* Inside your Z-NOMP directory (where the init.js script is) do `git pull` to get the latest Z-NOMP code.
* Remove the dependenices by deleting the `node_modules` directory with `rm -r node_modules`.
* Run `npm update` to force updating/reinstalling of the dependencies.
* Compare your `config.json` and `pool_configs/coin.json` configurations to the latest example ones in this repo or the ones in the setup instructions where each config field is explained. <b>You may need to modify or add any new changes.</b>


Credits
-------
### Z-NOMP
* [Joshua Yabut / movrcx](https://github.com/joshuayabut)
* [Aayan L / anarch3](https://github.com/aayanl)
* [hellcatz](https://github.com/hellcatz)

### NOMP
* [Matthew Little / zone117x](https://github.com/zone117x) - developer of NOMP
* [Jerry Brady / mintyfresh68](https://github.com/bluecircle) - got coin-switching fully working and developed proxy-per-algo feature
* [Tony Dobbs](http://anthonydobbs.com) - designs for front-end and created the NOMP logo
* [LucasJones](//github.com/LucasJones) - got p2p block notify working and implemented additional hashing algos
* [vekexasia](//github.com/vekexasia) - co-developer & great tester
* [TheSeven](//github.com/TheSeven) - answering an absurd amount of my questions and being a very helpful gentleman
* [UdjinM6](//github.com/UdjinM6) - helped implement fee withdrawal in payment processing
* [Alex Petrov / sysmanalex](https://github.com/sysmanalex) - contributed the pure C block notify script
* [svirusxxx](//github.com/svirusxxx) - sponsored development of MPOS mode
* [icecube45](//github.com/icecube45) - helping out with the repo wiki
* [Fcases](//github.com/Fcases) - ordered me a pizza <3
* Those that contributed to [node-stratum-pool](//github.com/zone117x/node-stratum-pool#credits)


License
-------
Released under the MIT License. See LICENSE file.
