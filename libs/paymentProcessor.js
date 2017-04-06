var fs = require('fs');

var redis = require('redis');
var async = require('async');

var Stratum = require('stratum-pool');
var util = require('stratum-pool/lib/util.js');


module.exports = function(logger){

    var poolConfigs = JSON.parse(process.env.pools);

    var enabledPools = [];

    Object.keys(poolConfigs).forEach(function(coin) {
        var poolOptions = poolConfigs[coin];
        if (poolOptions.paymentProcessing &&
            poolOptions.paymentProcessing.enabled)
            enabledPools.push(coin);
    });

    async.filter(enabledPools, function(coin, callback){
        SetupForPool(logger, poolConfigs[coin], function(setupResults){
            callback(null, setupResults);
        });
    }, function(err, results){
        results.forEach(function(coin){

            var poolOptions = poolConfigs[coin];
            var processingConfig = poolOptions.paymentProcessing;
            var logSystem = 'Payments';
            var logComponent = coin;

            logger.debug(logSystem, logComponent, 'Payment processing setup to run every '
                + processingConfig.paymentInterval + ' second(s) with daemon ('
                + processingConfig.daemon.user + '@' + processingConfig.daemon.host + ':' + processingConfig.daemon.port
                + ') and redis (' + poolOptions.redis.host + ':' + poolOptions.redis.port + ')');

        });
    });
};

function SetupForPool(logger, poolOptions, setupFinished){


    var coin = poolOptions.coin.name;
    var processingConfig = poolOptions.paymentProcessing;

    var logSystem = 'Payments';
    var logComponent = coin;
    var opidCount = 0;

    var minConfShield = 3;
    var minConfPayout = 10;

    var requireShielding = poolOptions.coin.requireShielding === true;
    var fee = poolOptions.coin.txfee;

    logger.special(logSystem, logComponent, logComponent + ' requireShielding: ' + requireShielding);

    var daemon = new Stratum.daemon.interface([processingConfig.daemon], function(severity, message){
        logger[severity](logSystem, logComponent, message);
    });
    var redisClient = redis.createClient(poolOptions.redis.port, poolOptions.redis.host);

    var magnitude;
    var minPaymentSatoshis;
    var coinPrecision;

    var paymentInterval;

    function validateAddress (callback){
        daemon.cmd('validateaddress', [poolOptions.address], function(result) {
            if (result.error){
                logger.error(logSystem, logComponent, 'Error with payment processing daemon ' + JSON.stringify(result.error));
                callback(true);
            }
            else if (!result.response || !result.response.ismine) {
                logger.error(logSystem, logComponent,
                    'Daemon does not own pool address - payment processing can not be done with this daemon, '
                    + JSON.stringify(result.response));
                callback(true);
            }
            else{
                callback()
            }
        }, true);
    }
    function validateTAddress (callback) {
        daemon.cmd('validateaddress', [poolOptions.tAddress], function(result) {
            if (result.error){
                logger.error(logSystem, logComponent, 'Error with payment processing daemon ' + JSON.stringify(result.error));
                callback(true);
            }
            else if (!result.response || !result.response.ismine) {
                logger.error(logSystem, logComponent,
                    'Daemon does not own pool address - payment processing can not be done with this daemon, '
                    + JSON.stringify(result.response));
                callback(true);
            }
            else{
                callback()
            }
        }, true);
     }
     function validateZAddress (callback) {
        daemon.cmd('z_validateaddress', [poolOptions.zAddress], function(result) {
            if (result.error){
                logger.error(logSystem, logComponent, 'Error with payment processing daemon ' + JSON.stringify(result.error));
                callback(true);
            }
            else if (!result.response || !result.response.ismine) {
                logger.error(logSystem, logComponent,
                    'Daemon does not own pool address - payment processing can not be done with this daemon, '
                    + JSON.stringify(result.response));
                callback(true);
            }
            else{
                callback()
            }
        }, true);
    }
    function getBalance(callback){
        daemon.cmd('getbalance', [], function(result){
            if (result.error){
                return callback(true);
            }
            try {
                var d = result.data.split('result":')[1].split(',')[0].split('.')[1];
                magnitude = parseInt('10' + new Array(d.length).join('0'));
                minPaymentSatoshis = parseInt(processingConfig.minimumPayment * magnitude);
                coinPrecision = magnitude.toString().length - 1;
            }
            catch(e){
                logger.error(logSystem, logComponent, 'Error detecting number of satoshis in a coin, cannot do payment processing. Tried parsing: ' + result.data);
                return callback(true);
            }
            callback();
        }, true, true);
    }

    function asyncComplete(err){
        if (err){
            setupFinished(false);
            return;
        }
        if (paymentInterval) {
            clearInterval(paymentInterval);
        }
        paymentInterval = setInterval(function(){
            try {
                processPayments();
            } catch(e){
                throw e;
            }
        }, processingConfig.paymentInterval * 1000);
        setTimeout(processPayments, 100);
        setupFinished(true);
    }

    async.parallel([validateAddress, validateTAddress, validateZAddress, getBalance], asyncComplete);

    //get t_address coinbalance
    function listUnspent (addr, notAddr, minConf, displayBool, callback) {
        if (addr !== null) {
            var args = [minConf, 99999999, [addr]];
        } else {
            addr = 'Payout wallet';
            var args = [minConf, 99999999];
        }
        daemon.cmd('listunspent', args, function (result) {
            //Check if payments failed because wallet doesn't have enough coins to pay for tx fees
            if (result.error) {
                logger.error(logSystem, logComponent, 'Error trying to get t-addr ['+addr+'] balance with RPC listunspent.'
                    + JSON.stringify(result.error));
                callback = function (){};
                callback(true);
            }
            else {
                var tBalance = 0;
                if (result[0].response != null && result[0].response.length > 0) {
                    for (var i = 0, len = result[0].response.length; i < len; i++) {
                        if (result[0].response[i].address !== notAddr) {
                            tBalance = tBalance + (result[0].response[i].amount * magnitude);
                        }
                    }
                }
                if (displayBool === true) {
                    logger.special(logSystem, logComponent, addr+' balance of ' + (tBalance / magnitude).toFixed(8));
                }
                callback(null, tBalance.toFixed(8));
            }
        });
    }

    // get z_address coinbalance
    function listUnspentZ (addr, minConf, displayBool, callback) {
        daemon.cmd('z_getbalance', [addr, minConf], function (result) {
            //Check if payments failed because wallet doesn't have enough coins to pay for tx fees
            if (result[0].error) {
                logger.error(logSystem, logComponent, 'Error trying to get coin balance with RPC z_getbalance.' + JSON.stringify(result[0].error));
                callback = function (){};
                callback(true);
            }
            else {
                var zBalance = 0;
                if (result[0].response != null) {
                    zBalance = result[0].response;
                }
                if (displayBool === true) {
                    logger.special(logSystem, logComponent, addr.substring(0,14) + '...' + addr.substring(addr.length - 14) + ' balance: '+(zBalance).toFixed(8));
                }
                callback(null, (zBalance * magnitude).toFixed(8));
            }
        });
    }

    //send t_address balance to z_address
    function sendTToZ (callback, tBalance) {
        if (callback === true)
            return;
        if ((tBalance - 10000) < 0)
            return;

        // do not allow more than a single z_sendmany operation at a time
        if (opidCount > 0) {
            logger.warning(logSystem, logComponent, 'sendTToZ is waiting, too many z_sendmany operations already in progress.');
            return;
        }

        var amount = balanceRound((tBalance - 10000) / magnitude);
        var params = [poolOptions.address, [{'address': poolOptions.zAddress, 'amount': amount}]];
        daemon.cmd('z_sendmany', params,
            function (result) {
                //Check if payments failed because wallet doesn't have enough coins to pay for tx fees
                if (result.error) {
                    logger.error(logSystem, logComponent, 'Error trying to shield mined balance ' + JSON.stringify(result.error));
                    callback = function (){};
                    callback(true);
                }
                else {
                    opidCount++;
                    logger.special(logSystem, logComponent, 'Shield mined balance ' + amount);
                    callback = function (){};
                    callback(null);
                }
            }
        );
    }

    // send z_address balance to t_address
    function sendZToT (callback, zBalance) {
        if (callback === true)
            return;
        if ((zBalance - 10000) < 0)
            return;

        // do not allow more than a single z_sendmany operation at a time
        if (opidCount > 0) {
            logger.warning(logSystem, logComponent, 'sendZToT is waiting, too many z_sendmany operations already in progress.');
            return;
        }

        var amount = balanceRound((zBalance - 10000) / magnitude);
        // no more than 100 ZEC at a time
        if (amount > 100.0)
            amount = 100.0;

        var params = [poolOptions.zAddress, [{'address': poolOptions.tAddress, 'amount': amount}]];
        daemon.cmd('z_sendmany', params,
            function (result) {
                //Check if payments failed because wallet doesn't have enough coins to pay for tx fees
                if (result.error) {
                    logger.error(logSystem, logComponent, 'Error trying to send z_address coin balance to payout t_address.'
                        + JSON.stringify(result.error));
                    callback = function (){};
                    callback(true);
                }
                else {
                    opidCount++;
                    logger.special(logSystem, logComponent, 'Unshield funds for payout ' + amount);
                    callback = function (){};
                    callback(null);
                }
            }
        );
    }


    function cacheNetworkStats () {
        var params = null;
        daemon.cmd('getmininginfo', params,
            function (result) {
                var finalRedisCommands = [];
                var coin = logComponent;

                if (result.error) {
                    logger.error(logSystem, logComponent, 'Error with RPC call `getmininginfo`'
                        + JSON.stringify(result.error));
                    return;
                } else {
                    if (result[0].response.blocks !== null) {
                        finalRedisCommands.push(['hset', coin + ':stats', 'networkBlocks', result[0].response.blocks]);
                        finalRedisCommands.push(['hset', coin + ':stats', 'networkDiff', result[0].response.difficulty]);
                        finalRedisCommands.push(['hset', coin + ':stats', 'networkSols', result[0].response.networksolps]);
                    } else {
                        logger.error(logSystem, logComponent, "Error parse RPC call reponse.blocks tp `getmininginfo`." + JSON.stringify(result[0].response));
                    }
                }

                daemon.cmd('getnetworkinfo', params,
                    function (result) {
                        if (result.error) {
                            logger.error(logSystem, logComponent, 'Error with RPC call `getnetworkinfo`'
                                + JSON.stringify(result.error));
                            return;
                        } else {
                            if (result[0].response !== null) {
                                finalRedisCommands.push(['hset', coin + ':stats', 'networkConnections', result[0].response.connections]);
                                finalRedisCommands.push(['hset', coin + ':stats', 'networkVersion', result[0].response.version]);
                                finalRedisCommands.push(['hset', coin + ':stats', 'networkSubVersion', result[0].response.subversion]);
                                finalRedisCommands.push(['hset', coin + ':stats', 'networkProtocolVersion', result[0].response.protocolversion]);
                            } else {
                                logger.error(logSystem, logComponent, "Error parse RPC call response to `getnetworkinfo`." + JSON.stringify(result[0].response));
                            }
                        }
                        redisClient.multi(finalRedisCommands).exec(function(error, results){
                            if (error){
                                logger.error(logSystem, logComponent, 'Error update coin stats to redis ' + JSON.stringify(error));
                                return;
                            }
                        });
                    }
                );
            }
        );
    }

    // run coinbase coin transfers every x minutes
    var intervalState = 0; // do not send ZtoT and TtoZ and same time, this results in operation failed!
    var interval = poolOptions.walletInterval * 60 * 1000; // run every x minutes
    setInterval(function() {
        // shielding not required for some equihash coins
        if (requireShielding === true) {
            intervalState++;
            switch (intervalState) {
                case 1:
                    listUnspent(poolOptions.address, null, minConfShield, false, sendTToZ);
                    break;
                default:
                    listUnspentZ(poolOptions.zAddress, minConfShield, false, sendZToT);
                    intervalState = 0;
                    break;
            }
        }
        // update network stats using coin daemon
        cacheNetworkStats();
    }, interval);

    // check operation statuses every x seconds
    var opid_interval =  poolOptions.walletInterval * 1000;
    // shielding not required for some equihash coins
    if (requireShielding === true) {
        setInterval(function(){
           var checkOpIdSuccessAndGetResult = function(ops) {
              ops.forEach(function(op, i){
                if (op.status == "success" || op.status == "failed") {
                    daemon.cmd('z_getoperationresult', [[op.id]], function (result) {
                        if (result.error) {
                            logger.warning(logSystem, logComponent, 'Unable to get payment operation id result ' + JSON.stringify(result));
                        }
                        if (result.response) {
                            if (opidCount > 0) {
                                opidCount = 0;
                            }
                            if (op.status == "failed") {
                                if (op.error) {
                                  logger.error(logSystem, logComponent, "Payment operation failed " + op.id + " " + op.error.code +", " + op.error.message);
                                } else {
                                  logger.error(logSystem, logComponent, "Payment operation failed " + op.id);
                                }
                            } else {
                                logger.special(logSystem, logComponent, 'Payment operation success ' + op.id + '  txid: ' + op.result.txid);
                            }
                        }
                    }, true, true);
                } else if (op.status == "executing") {
                    if (opidCount == 0) {
                        opidCount++;
                        logger.special(logSystem, logComponent, 'Payment operation in progress ' + op.id );
                    }
                }
              });
           };
           daemon.cmd('z_getoperationstatus', null, function (result) {
              if (result.error) {
                logger.warning(logSystem, logComponent, 'Unable to get operation ids for clearing.');
              }
              if (result.response) {
                checkOpIdSuccessAndGetResult(result.response);
              }
           }, true, true);
        }, opid_interval);
    }

    var satoshisToCoins = function(satoshis){
        return parseFloat((satoshis / magnitude).toFixed(coinPrecision));
    };

    var coinsToSatoshies = function(coins){
        return coins * magnitude;
    };

    function balanceRound(number) {
    return parseFloat((Math.round(number * 100000000) / 100000000).toFixed(8));
    }

    /* Deal with numbers in smallest possible units (satoshis) as much as possible. This greatly helps with accuracy
       when rounding and whatnot. When we are storing numbers for only humans to see, store in whole coin units. */

    var processPayments = function(){

        var startPaymentProcess = Date.now();

        var timeSpentRPC = 0;
        var timeSpentRedis = 0;

        var startTimeRedis;
        var startTimeRPC;

        var startRedisTimer = function(){ startTimeRedis = Date.now() };
        var endRedisTimer = function(){ timeSpentRedis += Date.now() - startTimeRedis };

        var startRPCTimer = function(){ startTimeRPC = Date.now(); };
        var endRPCTimer = function(){ timeSpentRPC += Date.now() - startTimeRedis };

        async.waterfall([

            /* Call redis to get an array of rounds - which are coinbase transactions and block heights from submitted
               blocks. */
            function(callback){
                startRedisTimer();
                redisClient.multi([
                    ['hgetall', coin + ':balances'],
                    ['smembers', coin + ':blocksPending']
                ]).exec(function(error, results){
                    endRedisTimer();

                    if (error){
                        logger.error(logSystem, logComponent, 'Could not get blocks from redis ' + JSON.stringify(error));
                        callback(true);
                        return;
                    }

                    var workers = {};
                    for (var w in results[0]){
                        workers[w] = {balance: coinsToSatoshies(parseFloat(results[0][w]))};
                    }

                    var rounds = results[1].map(function(r){
                        var details = r.split(':');
                        return {
                            blockHash: details[0],
                            txHash: details[1],
                            height: details[2],
                            serialized: r
                        };
                    });

                    callback(null, workers, rounds);
                });
            },


            /* Does a batch rpc call to daemon with all the transaction hashes to see if they are confirmed yet.
               It also adds the block reward amount to the round object - which the daemon gives also gives us. */
            function(workers, rounds, callback){

                // first verify block confirmations by block hash
                var batchRPCcommand2 = rounds.map(function(r){
                    return ['getblock', [r.blockHash]];
                });
                // guarantee a response for batchRPCcommand2
                batchRPCcommand2.push(['getblockcount']);

                startRPCTimer();
                daemon.batchCmd(batchRPCcommand2, function(error, blockDetails){
                    endRPCTimer();

                    // error getting block info by hash?
                    if (error || !blockDetails){
                        logger.error(logSystem, logComponent, 'Check finished - daemon rpc error with batch getblock '
                            + JSON.stringify(error));
                        callback(true);
                        return;
                    }

                    // update confirmations in redis for pending blocks
                    var confirmsUpdate = blockDetails.map(function(b) {
                        if (b.result != null && b.result.confirmations > 0) {
                            if (b.result.confirmations > 100) {
                                return ['hdel', logComponent + ':blocksPendingConfirms', b.result.hash];
                            }
                            return ['hset', logComponent + ':blocksPendingConfirms', b.result.hash, b.result.confirmations];
                        }
                        return null;
                    });

                    // filter nulls, last item is always null...
                    confirmsUpdate = confirmsUpdate.filter(function(val) { return val !== null; });
                    // guarantee at least one redis update
                    if (confirmsUpdate.length < 1)
                        confirmsUpdate.push(['hset', logComponent + ':blocksPendingConfirms', 0, 0]);

                    startRedisTimer();
                    redisClient.multi(confirmsUpdate).exec(function(error, updated){
                        endRedisTimer();

                        if (error){
                            logger.error(logSystem, logComponent, 'failed to update pending block confirmations'
                                + JSON.stringify(error));
                            callback(true);
                            return;
                        }

                        // check for invalid blocks by block hash
                        blockDetails.forEach(function(block, i) {
                            // this is just the response from getblockcount
                            if (i === blockDetails.length - 1){
                                return;
                            }
                            // help track duplicate or invalid blocks by block hash
                            if (block && block.result && block.result.hash) {
                                // find the round for this block hash
                                for (var k=0; k < rounds.length; k++) {
                                    if (rounds[k].blockHash == block.result.hash) {
                                        var round = rounds[k];
                                        var dupFound = false;
                                        // duplicate, invalid, kicked, orphaned blocks will have negative confirmations
                                        if (block.result.confirmations < 0) {
                                            // check if this is an invalid duplicate
                                            // we need to kick invalid duplicates now, as this will cause a double payout...
                                            for (var d=0; d < rounds.length; d++) {
                                                if (rounds[d].height == block.result.height && rounds[d].blockHash != block.result.hash) {
                                                    logger.warning(logSystem, logComponent, 'Kicking invalid duplicate block ' +round.height + ' > ' + round.blockHash);
                                                    dupFound = true;
                                                    // kick this round now, its completely invalid!
                                                    var kickNow = [];
                                                    kickNow.push(['smove', coin + ':blocksPending', coin + ':blocksDuplicate', round.serialized]);
                                                    startRedisTimer();
                                                    redisClient.multi(kickNow).exec(function(error, kicked){
                                                        endRedisTimer();
                                                        if (error){
                                                            logger.error(logSystem, logComponent, 'Error could not kick invalid duplicate block ' + JSON.stringify(kicked));
                                                        }
                                                    });
                                                    // filter the duplicate out now, just in case we are actually paying this time around...
                                                    rounds = rounds.filter(function(item){ return item.txHash != round.txHash; });
                                                }
                                            }
                                            // unknown reason why this block failed, possible orphan or kicked soon
                                            // not sure if we should take any action or just wait it out...
                                            if (!dupFound) {
                                                logger.warning(logSystem, logComponent, 'Daemon reports negative confirmations '+block.result.confirmations+' for block: ' +round.height + ' > ' + round.blockHash);
                                            }
                                        }
                                    }
                                }
                            }
                        });

                        // now check block transaction ids
                        var batchRPCcommand = rounds.map(function(r){
                            return ['gettransaction', [r.txHash]];
                        });
                        // guarantee a response for batchRPCcommand
                        batchRPCcommand.push(['getaccount', [poolOptions.address]]);

                        startRPCTimer();
                        daemon.batchCmd(batchRPCcommand, function(error, txDetails){
                            endRPCTimer();

                            if (error || !txDetails){
                                logger.error(logSystem, logComponent, 'Check finished - daemon rpc error with batch gettransactions '
                                    + JSON.stringify(error));
                                callback(true);
                                return;
                            }

                            var addressAccount = "";

                            // check for transaction errors and generated coins
                            txDetails.forEach(function(tx, i){

                                if (i === txDetails.length - 1){
                                    addressAccount = tx.result;
                                    return;
                                }

                                var round = rounds[i];
                                if (tx.error && tx.error.code === -5){
                                    logger.warning(logSystem, logComponent, 'Daemon reports invalid transaction: ' + round.txHash);
                                    round.category = 'kicked';
                                    return;
                                }
                                else if (!tx.result.details || (tx.result.details && tx.result.details.length === 0)){
                                    logger.warning(logSystem, logComponent, 'Daemon reports no details for transaction: ' + round.txHash);
                                    round.category = 'kicked';
                                    return;
                                }
                                else if (tx.error || !tx.result){
                                    logger.error(logSystem, logComponent, 'Odd error with gettransaction ' + round.txHash + ' '
                                        + JSON.stringify(tx));
                                    return;
                                }

                                var generationTx = tx.result.details.filter(function(tx){
                                    return tx.address === poolOptions.address;
                                })[0];

                                if (!generationTx && tx.result.details.length === 1){
                                    generationTx = tx.result.details[0];
                                }

                                if (!generationTx){
                                    logger.error(logSystem, logComponent, 'Missing output details to pool address for transaction '
                                        + round.txHash);
                                    return;
                                }

                                round.category = generationTx.category;
                                if (round.category === 'generate') {
                                    round.reward = balanceRound(generationTx.amount - fee) || balanceRound(generationTx.value - fee); // TODO: Adjust fees to be dynamic
                                }

                            });

                            var canDeleteShares = function(r){
                                for (var i = 0; i < rounds.length; i++){
                                    var compareR = rounds[i];
                                    if ((compareR.height === r.height)
                                        && (compareR.category !== 'kicked')
                                        && (compareR.category !== 'orphan')
                                        && (compareR.serialized !== r.serialized)){
                                        return false;
                                    }
                                }
                                return true;
                            };

                            //Filter out all rounds that are immature (not confirmed or orphaned yet)
                            rounds = rounds.filter(function(r){
                                switch (r.category) {
                                    case 'orphan':
                                    case 'kicked':
                                        r.canDeleteShares = canDeleteShares(r);
                                    case 'generate':
                                        return true;
                                    default:
                                        return false;
                                }
                            });

                            fee = fee * 1e8;

                            // calculate what the pool owes its miners
                            var totalOwed = parseInt(0);
                            for (var i = 0; i < rounds.length; i++) {
                                totalOwed = totalOwed + Math.round(rounds[i].reward * magnitude) - fee; // TODO: make tx fees dynamic
                            }

                            var notAddr = null;
                            if (requireShielding === true) {
                                notAddr = poolOptions.address;
                            }

                            // check if we have enough tAddress funds to brgin payment processing
                            listUnspent(null, notAddr, minConfPayout, false, function (error, tBalance){
                                if (error) {
                                    logger.error(logSystem, logComponent, 'Error checking pool balance before payouts. (Unable to begin payment process)');
                                    return callback(true);
                                } else if (tBalance < totalOwed) {
                                    logger.error(logSystem, logComponent,  'Insufficient pool funds to being payment process; '+(tBalance / magnitude).toFixed(8) + ' < ' + (totalOwed / magnitude).toFixed(8)+'. (Possibly due to pending txs) ');
                                    return callback(true);
                                } else {
                                    // zcash daemon does not support account feature
                                    addressAccount = "";
                                    callback(null, workers, rounds, addressAccount);
                                }
                            })

                        });
                    });
                });
            },


            /* Does a batch redis call to get shares contributed to each round. Then calculates the reward
               amount owned to each miner for each round. */
            function(workers, rounds, addressAccount, callback){

                var shareLookups = rounds.map(function(r){
                    return ['hgetall', coin + ':shares:round' + r.height]
                });

                startRedisTimer();
                redisClient.multi(shareLookups).exec(function(error, allWorkerShares){
                    endRedisTimer();

                    if (error){
                        callback('Check finished - redis error with multi get rounds share');
                        return;
                    }

                    rounds.forEach(function(round, i){
                        var workerShares = allWorkerShares[i];

                        if (!workerShares){
                            logger.error(logSystem, logComponent, 'No worker shares for round: '
                                + round.height + ' blockHash: ' + round.blockHash);
                            return;
                        }

                        switch (round.category){
                            case 'kicked':
                            case 'orphan':
                                round.workerShares = workerShares;
                                break;

                            case 'generate':
                                /* We found a confirmed block! Now get the reward for it and calculate how much
                                   we owe each miner based on the shares they submitted during that block round. */
                                var reward = parseInt(round.reward * magnitude);

                                var totalShares = Object.keys(workerShares).reduce(function(p, c){
                                    return p + parseFloat(workerShares[c])
                                }, 0);

                                for (var workerAddress in workerShares){
                                    var percent = parseFloat(workerShares[workerAddress]) / totalShares;
                                    var workerRewardTotal = Math.floor(reward * percent);
                                    var worker = workers[workerAddress] = (workers[workerAddress] || {});
                                    worker.totalShares = (worker.totalShares || 0) + parseFloat(workerShares[workerAddress]);
                                    worker.reward = (worker.reward || 0) + workerRewardTotal;
                                }
                                break;
                        }
                    });

                    callback(null, workers, rounds, addressAccount);
                });
            },


            /* Calculate if any payments are ready to be sent and trigger them sending
             Get balance different for each address and pass it along as object of latest balances such as
             {worker1: balance1, worker2, balance2}
             when deciding the sent balance, it the difference should be -1*amount they had in db,
             if not sending the balance, the differnce should be +(the amount they earned this round)
             */
            function(workers, rounds, addressAccount, callback) {

                var trySend = function (withholdPercent) {
                    var addressAmounts = {};
                    var minerTotals = {};
                    var totalSent = 0;
                    var totalShares = 0;
                    // total up miner's balances
                    for (var w in workers) {
                        var worker = workers[w];
                        totalShares += (worker.totalShares || 0)
                        worker.balance = worker.balance || 0;
                        worker.reward = worker.reward || 0;
                        var toSend = balanceRound(satoshisToCoins(Math.floor((worker.balance + worker.reward) * (1 - withholdPercent))));
                        var address = worker.address = (worker.address || getProperAddress(w.split('.')[0]));
                        if (minerTotals[address] != null && minerTotals[address] > 0) {
                            minerTotals[address] = balanceRound(minerTotals[address] + toSend);
                        } else {
                            minerTotals[address] = toSend;
                        }
                    }
                    // now process each workers balance, and pay the miner
                    for (var w in workers) {
                        var worker = workers[w];
                        worker.balance = worker.balance || 0;
                        worker.reward = worker.reward || 0;
                        var toSend = Math.floor((worker.balance + worker.reward) * (1 - withholdPercent));
                        var address = worker.address = (worker.address || getProperAddress(w.split('.')[0]));
                        // if miners total is enough, go ahead and add this worker balance
                        if (minerTotals[address] >= satoshisToCoins(minPaymentSatoshis)) {
                            totalSent += toSend;
                            worker.sent = balanceRound(satoshisToCoins(toSend));
                            worker.balanceChange = Math.min(worker.balance, toSend) * -1;
                            // multiple workers may have same address, add them up
                            if (addressAmounts[address] != null && addressAmounts[address] > 0) {
                                addressAmounts[address] = balanceRound(addressAmounts[address] + worker.sent);
                            } else {
                                addressAmounts[address] = worker.sent;
                            }
                        }
                        else {
                            worker.balanceChange = Math.max(toSend - worker.balance, 0);
                            worker.sent = 0;
                        }
                    }

                    // if no payouts...continue to next set of callbacks
                    if (Object.keys(addressAmounts).length === 0){
                        callback(null, workers, rounds);
                        return;
                    }
                    /*
                    var undoPaymentsOnError = function(workers) {
                        totalSent = 0;
                        // TODO, set round.category to immature, to attempt to pay again
                        // we did not send anything to any workers
                        for (var w in workers) {
                            var worker = workers[w];
                            if (worker.sent > 0) {
                                worker.balanceChange = 0;
                                worker.sent = 0;
                            }
                        }
                    };
                    */
                    // perform the sendmany operation
                    daemon.cmd('sendmany', ["", addressAmounts], function (result) {
                        // check for failed payments, there are many reasons
                        if (result.error && result.error.code === -6) {
                            // not enough minerals...
                            var higherPercent = withholdPercent + 0.01;
                            logger.warning(logSystem, logComponent, 'Not enough funds to cover the tx fees for sending out payments, decreasing rewards by '
                                + (higherPercent * 100) + '% and retrying');

                            trySend(higherPercent);
                        }
                        else if (result.error && result.error.code === -5) {
                            // invalid address specified in addressAmounts array
                            logger.error(logSystem, logComponent, 'Error sending payments ' + result.error.message);
                            //undoPaymentsOnError(workers);
                            callback(true);
                            return;
                        }
                        else if (result.error && result.error.message != null) {
                            // unknown error from daemon
                            logger.error(logSystem, logComponent, 'Error sending payments ' + result.error.message);
                            //undoPaymentsOnError(workers);
                            callback(true);
                            return;
                        }
                        else if (result.error) {
                            // some other unknown error
                            logger.error(logSystem, logComponent, 'Error sending payments ' + JSON.stringify(result.error));
                            //undoPaymentsOnError(workers);
                            callback(true);
                            return;
                        }
                        else {

                            // make sure sendmany gives us back a txid
                            var txid = null;
                            if (result.response) {
                                txid = result.response;
                            }
                            if (txid != null) {

                                // it worked, congrats on your pools payout ;)
                                logger.special(logSystem, logComponent, 'Sent ' + (totalSent / magnitude).toFixed(8)
                                    + ' to ' + Object.keys(addressAmounts).length + ' miners; txid: '+txid);

                                if (withholdPercent > 0) {
                                    logger.warning(logSystem, logComponent, 'Had to withhold ' + (withholdPercent * 100)
                                        + '% of reward from miners to cover transaction fees. '
                                        + 'Fund pool wallet with coins to prevent this from happening');
                                }

                                // save payments data to redis
                                var paymentBlocks = rounds.map(function(r){
                                    return parseInt(r.height);
                                });
                                var paymentsUpdate = [];
                                var paymentsData = {time:Date.now(), txid:txid, shares:totalShares, paid:balanceRound(totalSent / magnitude),  miners:Object.keys(addressAmounts).length, blocks: paymentBlocks, amounts: addressAmounts};
                                paymentsUpdate.push(['zadd', logComponent + ':payments', Date.now(), JSON.stringify(paymentsData)]);
                                startRedisTimer();
                                redisClient.multi(paymentsUpdate).exec(function(error, payments){
                                    endRedisTimer();
                                    if (error){
                                        logger.error(logSystem, logComponent, 'Error redis save payments data ' + JSON.stringify(payments));
                                    }
                                    callback(null, workers, rounds);
                                });

                            } else {

                                clearInterval(paymentInterval);

                                logger.error(logSystem, logComponent, 'Error RPC sendmany did not return txid '
                                    + JSON.stringify(result) + 'Disabling payment processing to prevent possible double-payouts.');

                                callback(true);
                                return;
                            }
                        }
                    }, true, true);

                };

                trySend(0);

            },
            function(workers, rounds, callback){

                var totalPaid = parseFloat(0);

                var balanceUpdateCommands = [];
                var workerPayoutsCommand = [];

                // update worker paid/balance stats
                for (var w in workers) {
                    var worker = workers[w];
                    if (worker.balanceChange !== 0){
                        balanceUpdateCommands.push([
                            'hincrbyfloat',
                            coin + ':balances',
                            w,
                            balanceRound(satoshisToCoins(worker.balanceChange))
                        ]);
                    }
                    if (worker.sent !== 0){
                        workerPayoutsCommand.push(['hincrbyfloat', coin + ':payouts', w, balanceRound(worker.sent)]);
                        totalPaid = balanceRound(totalPaid + worker.sent);
                    }
                }

                var movePendingCommands = [];
                var roundsToDelete = [];
                var orphanMergeCommands = [];

                var moveSharesToCurrent = function(r){
                    var workerShares = r.workerShares;
                    if (workerShares != null) {
                        Object.keys(workerShares).forEach(function(worker){
                            orphanMergeCommands.push(['hincrby', coin + ':shares:roundCurrent', worker, workerShares[worker]]);
                        });
                    }
                };

                // handle the round
                rounds.forEach(function(r){
                    switch(r.category){
                        case 'kicked':
                            movePendingCommands.push(['smove', coin + ':blocksPending', coin + ':blocksKicked', r.serialized]);
                        case 'orphan':
                            movePendingCommands.push(['smove', coin + ':blocksPending', coin + ':blocksOrphaned', r.serialized]);
                            if (r.canDeleteShares){
                                moveSharesToCurrent(r);
                                roundsToDelete.push(coin + ':shares:round' + r.height);
                            }
                            return;
                        case 'generate':
                            movePendingCommands.push(['smove', coin + ':blocksPending', coin + ':blocksConfirmed', r.serialized]);
                            roundsToDelete.push(coin + ':shares:round' + r.height);
                            return;
                    }
                });

                var finalRedisCommands = [];

                if (movePendingCommands.length > 0)
                    finalRedisCommands = finalRedisCommands.concat(movePendingCommands);

                if (orphanMergeCommands.length > 0)
                    finalRedisCommands = finalRedisCommands.concat(orphanMergeCommands);

                if (balanceUpdateCommands.length > 0)
                    finalRedisCommands = finalRedisCommands.concat(balanceUpdateCommands);

                if (workerPayoutsCommand.length > 0)
                    finalRedisCommands = finalRedisCommands.concat(workerPayoutsCommand);

                if (roundsToDelete.length > 0)
                    finalRedisCommands.push(['del'].concat(roundsToDelete));

                if (totalPaid !== 0)
                    finalRedisCommands.push(['hincrbyfloat', coin + ':stats', 'totalPaid', balanceRound(totalPaid)]);

                if (finalRedisCommands.length === 0){
                    callback();
                    return;
                }

                startRedisTimer();
                redisClient.multi(finalRedisCommands).exec(function(error, results){
                    endRedisTimer();
                    if (error){
                        clearInterval(paymentInterval);
                        logger.error(logSystem, logComponent,
                                'Payments sent but could not update redis. ' + JSON.stringify(error)
                                + ' Disabling payment processing to prevent possible double-payouts. The redis commands in '
                                + coin + '_finalRedisCommands.txt must be ran manually');
                        fs.writeFile(coin + '_finalRedisCommands.txt', JSON.stringify(finalRedisCommands), function(err){
                            logger.error('Could not write finalRedisCommands.txt, you are fucked.');
                        });
                    }
                    callback();
                });
            }

        ], function(){

            var paymentProcessTime = Date.now() - startPaymentProcess;
            logger.debug(logSystem, logComponent, 'Finished interval - time spent: '
                + paymentProcessTime + 'ms total, ' + timeSpentRedis + 'ms redis, '
                + timeSpentRPC + 'ms daemon RPC');

        });
    };


    var getProperAddress = function(address){
        if (address.length === 40){
            return util.addressFromEx(poolOptions.address, address);
        }
        else return address;
    };

}
