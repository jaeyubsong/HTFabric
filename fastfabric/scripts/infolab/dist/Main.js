/*
 * SPDX-License-Identifier: Apache-2.0
 */
'use strict';
var __assign = (this && this.__assign) || function () {
    __assign = Object.assign || function(t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
            s = arguments[i];
            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                t[p] = s[p];
        }
        return t;
    };
    return __assign.apply(this, arguments);
};
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
exports.__esModule = true;
var fabric_network_1 = require("fabric-network");
var ChildProcess = require("child_process");
var FS = require("fs");
var Path = require("path");
var Redis = require("redis");
var util_1 = require("util");
var avg_send_tps = 0;
var total_send_tps = 0;
var send_tps_count = 0;
var BATCH = 1;
var STEP = 100;
var NUM_TPS = BATCH;
var NUM_THREAD = 40;
var NUM_CLIENT = 15;
var NUM_SUN = 9;
var NUM_APOLLO = 5;
var SUN_THREAD = 40;
var APOLLO_THREAD = 64;
var GOAL_TPS = 16000;
var GOAL_TPS_CUR_THREAD = GOAL_TPS / NUM_THREAD / NUM_CLIENT;
// const GOAL_TPS_CUR_THREAD = GOAL_TPS / (SUN_THREAD * NUM_SUN + APOLLO_THREAD * NUM_APOLLO)
// const GOAL_DURATION = Math.round(NUM_TPS / GOAL_TPS_CUR_THREAD * 1000)
var GOAL_DURATION = NUM_TPS / GOAL_TPS_CUR_THREAD * 1000; // Num_TPS가 몇ms에 한번씩 처리가 되어야 하나
// const BATCH = 3;
// const STEP = 2000;
// How many writes per 20 transactions // 
var Pw = 19;
// const NUM_TPS = 20
// const NUM_THREAD = 40
// const NUM_CLIENT = 17
// const NUM_SUN = 9
// const NUM_APOLLO = 5
// const SUN_THREAD = 40
// const APOLLO_THREAD = 64
// const GOAL_TPS = 10000
// const GOAL_TPS_CUR_THREAD = GOAL_TPS / NUM_THREAD / NUM_CLIENT
// // const GOAL_TPS_CUR_THREAD = GOAL_TPS / (SUN_THREAD * NUM_SUN + APOLLO_THREAD * NUM_APOLLO)
// // const GOAL_DURATION = Math.round(NUM_TPS / GOAL_TPS_CUR_THREAD * 1000)
// const GOAL_DURATION = NUM_TPS / GOAL_TPS_CUR_THREAD * 1000
// // const BATCH = 3;
// // const STEP = 2000;
// const BATCH = 3;
// const STEP = 100;
// const RATE = 0;
var TIMEOUT = 5000 * 10000;
var redis = Redis.createClient({
    host: "master.jjo.kr",
    port: 16379
});
var SET = util_1.promisify(redis.set).bind(redis);
var INPUT_LENGTH;
function readFile(path) {
    var stream = FS.createReadStream(path);
    var R = [];
    return new Promise(function (res) {
        stream.on('data', function (chunk) {
            R.push(chunk.toString());
        });
        stream.on('end', function () {
            stream.close();
            // R = [ R.join('') ];
            INPUT_LENGTH = R.length;
            console.log(process.argv[2], "읽기 완료", INPUT_LENGTH);
            res(R);
        });
    });
}
function main() {
    return __awaiter(this, void 0, void 0, function () {
        var TASK_ID, TASK_COUNT, INPUT, METHOD, maxConcat, concat, requestHandler, index, bcResps, ccpPath, ccpJSON, ccp, endorserAddresses, user, wallet, userExists, gateway, network, channel_1, client, redisSet, pair, chunk, count, nextCount, errorLife_1, i, input, length_1, requestCount, writeCount, durationStartTime, lastDur, prevExecIndex, prevExecLength, _loop_1, state_1, _a, error_1;
        var _this = this;
        return __generator(this, function (_b) {
            switch (_b.label) {
                case 0:
                    TASK_ID = parseInt(process.argv[2]);
                    TASK_COUNT = parseInt(process.argv[3]);
                    return [4 /*yield*/, readFile(process.argv[4])];
                case 1:
                    INPUT = _b.sent();
                    METHOD = process.argv[5];
                    concat = 0;
                    index = 0;
                    // await sleep_ms(getRandomArbitrary(1, 1000));
                    // await sleep_ms( (1000 / NUM_THREAD) *( TASK_ID % NUM_THREAD) + TASK_ID / NUM_CLIENT)
                    switch (METHOD) {
                        default:
                        case "send2":
                            maxConcat = 1;
                            requestHandler = function (chunk) { return ({
                                fcn: METHOD,
                                args: [chunk[0], chunk[1], chunk[2]]
                            }); };
                            break;
                        case "send4":
                            maxConcat = 2;
                            requestHandler = function (chunk) { return ({
                                fcn: METHOD,
                                args: [chunk[0], chunk[1], chunk[3], chunk[4], chunk[5]]
                            }); };
                            break;
                        case "send6":
                            maxConcat = 3;
                            requestHandler = function (chunk) { return ({
                                fcn: METHOD,
                                args: [chunk[0], chunk[1], chunk[3], chunk[4], chunk[6], chunk[7], chunk[8]]
                            }); };
                            break;
                        case "send8":
                            maxConcat = 4;
                            requestHandler = function (chunk) { return ({
                                fcn: METHOD,
                                args: [chunk[0], chunk[1], chunk[3], chunk[4], chunk[6], chunk[7], chunk[9], chunk[10], chunk[11]]
                            }); };
                            break;
                        case "readwrite4":
                            maxConcat = 4;
                            requestHandler = function (chunk) { return ({
                                fcn: METHOD,
                                args: [chunk[0], chunk[1], chunk[3], chunk[4], chunk[6], chunk[7], chunk[9], chunk[10]]
                            }); };
                            break;
                        case "readwrite8":
                            maxConcat = 8;
                            requestHandler = function (chunk) { return ({
                                fcn: METHOD,
                                args: [chunk[0], chunk[1], chunk[3], chunk[4], chunk[6], chunk[7], chunk[9], chunk[10], chunk[12], chunk[13], chunk[15], chunk[16], chunk[18], chunk[19], chunk[21], chunk[22]]
                            }); };
                            break;
                        case "fabric++":
                            maxConcat = 1;
                            requestHandler = function () {
                                var R = {
                                    fcn: METHOD,
                                    args: [String(index)]
                                };
                                index += 8;
                                return R;
                            };
                            break;
                    }
                    bcResps = [];
                    ccpPath = Path.resolve(__dirname, 'connection.json');
                    ccpJSON = FS.readFileSync(ccpPath, 'utf8');
                    ccp = JSON.parse(ccpJSON);
                    endorserAddresses = ChildProcess.execSync("bash printEndorsers.sh").toString().replace("\n", "").split(" ");
                    // console.log(`Endorser address: "${ccp.peers.address.url}"`);
                    // endorserAddresses.push("fast.org1.jjo.kr");
                    ccp.name = ccp.name.replace("CHANNEL", process.env.CHANNEL);
                    ccp.organizations.Org1.signedCert.path = ccp.organizations.Org1.signedCert.path.split("DOMAIN").join(process.env.PEER_DOMAIN);
                    ccp.orderers.address.url = ccp.orderers.address.url.replace("ADDRESS", process.env.ORDERER_ADDRESS + "." + process.env.ORDERER_DOMAIN);
                    ccp.peers.address.url = ccp.peers.address.url.replace("ADDRESS", endorserAddresses[TASK_ID % endorserAddresses.length]);
                    console.log("Endorser address: \"" + ccp.peers.address.url + "\"");
                    user = "Admin@" + process.env.PEER_DOMAIN;
                    console.log("Thread no.: " + TASK_ID + " -> " + ccp.peers.address.url);
                    wallet = new fabric_network_1.FileSystemWallet(Path.resolve(__dirname, "..", 'wallet'));
                    _b.label = 2;
                case 2:
                    _b.trys.push([2, 11, , 12]);
                    return [4 /*yield*/, wallet.exists(user)];
                case 3:
                    userExists = _b.sent();
                    if (!userExists) {
                        console.log("An identity for the user \"" + user + "\" does not exist in the wallet");
                        console.log('Run the registerUser.js application before retrying');
                        return [2 /*return*/];
                    }
                    gateway = new fabric_network_1.Gateway();
                    return [4 /*yield*/, gateway.connect(ccp, { wallet: wallet, identity: user, discovery: { enabled: false } })];
                case 4:
                    _b.sent();
                    return [4 /*yield*/, gateway.getNetwork(String(process.env.CHANNEL))];
                case 5:
                    network = _b.sent();
                    channel_1 = network.getChannel();
                    client = gateway.getClient();
                    redisSet = util_1.promisify(redis.set).bind(redis);
                    pair = /^(\S+)\s+(\S+)(?:\s+([.-\d]+))?$/gm;
                    chunk = [];
                    count = 0;
                    nextCount = STEP;
                    errorLife_1 = 5 * BATCH;
                    i = 0;
                    input = INPUT.shift();
                    length_1 = input.length;
                    requestCount = 1;
                    writeCount = TASK_ID;
                    durationStartTime = Date.now();
                    lastDur = 0;
                    prevExecIndex = 0;
                    prevExecLength = 0;
                    _loop_1 = function () {
                        var exec, startedAt, data, txId, curTps, curDuration, remainderTime_1, _a;
                        return __generator(this, function (_b) {
                            switch (_b.label) {
                                case 0:
                                    if (length_1 - prevExecIndex < 100 && INPUT.length) {
                                        input = input.slice(prevExecIndex + prevExecLength) + INPUT.shift();
                                        length_1 = input.length;
                                        pair = new RegExp(pair);
                                    }
                                    exec = pair.exec(input);
                                    prevExecIndex = exec.index;
                                    prevExecLength = exec[0].length;
                                    startedAt = void 0;
                                    if (!exec) {
                                        return [2 /*return*/, "break"];
                                    }
                                    // if(length - exec.index < 100 && INPUT.length){
                                    //   input = input.slice(exec.index + exec[0].length) + INPUT.shift();
                                    //   length = input.length;
                                    //   pair = new RegExp(pair);
                                    // }
                                    // console.log("input length: ", input.length)
                                    i++;
                                    if (i % TASK_COUNT !== TASK_ID) {
                                        return [2 /*return*/, "continue"];
                                    }
                                    chunk.push(exec[1], exec[2], String(Math.round(1e-8 * (exec[3] || 1e+8))));
                                    concat++;
                                    if (concat % maxConcat) {
                                        return [2 /*return*/, "continue"];
                                    }
                                    data = requestHandler(chunk);
                                    txId = client.newTransactionID();
                                    chunk = [];
                                    concat = 0;
                                    startedAt = Date.now();
                                    curTps = 0;
                                    if (!(requestCount > 0 && requestCount % NUM_TPS == 0)) return [3 /*break*/, 4];
                                    curDuration = Date.now() - durationStartTime;
                                    remainderTime_1 = (GOAL_DURATION * requestCount / NUM_TPS) - curDuration;
                                    if (!(remainderTime_1 > 0)) return [3 /*break*/, 2];
                                    // console.log(TASK_ID, " CUR TPS: ", requestCount / (curDuration / 1000))
                                    // console.log("Sleeping for ", remainderTime)
                                    return [4 /*yield*/, new Promise(function (resolve) { return setTimeout(resolve, remainderTime_1); })];
                                case 1:
                                    // console.log(TASK_ID, " CUR TPS: ", requestCount / (curDuration / 1000))
                                    // console.log("Sleeping for ", remainderTime)
                                    _b.sent();
                                    curDuration = Date.now() - durationStartTime;
                                    curTps = requestCount / (curDuration / 1000) * NUM_CLIENT * NUM_THREAD;
                                    return [3 /*break*/, 3];
                                case 2:
                                    curTps = requestCount / (curDuration / 1000) * NUM_CLIENT * NUM_THREAD;
                                    _b.label = 3;
                                case 3:
                                    total_send_tps += curTps;
                                    send_tps_count++;
                                    if (send_tps_count % 100 == 0) {
                                        avg_send_tps = total_send_tps / send_tps_count;
                                        console.log(TASK_ID + "\t" + curTps);
                                        console.log("requestCount: " + requestCount + "\tcurDuration:" + curDuration + "\tdiff:" + (curDuration - lastDur));
                                        lastDur = curDuration;
                                    }
                                    _b.label = 4;
                                case 4:
                                    requestCount++;
                                    return [4 /*yield*/, redisSet("ptst-" + txId.getTransactionID(), String(startedAt))];
                                case 5:
                                    _b.sent();
                                    if (!(bcResps.push(channel_1.sendTransactionProposal(__assign(__assign({ chaincodeId: String(process.env.CHAINCODE) }, data), { 
                                        // args: [ String(2 * i), String(2 * i + 1), "1" ],
                                        // args: [ String(2 * i), String(2 * i + 1), chunk[2] ],
                                        txId: txId }), TIMEOUT).then(function (res) {
                                        var _a;
                                        if (((_a = res[0]) === null || _a === void 0 ? void 0 : _a[0]) instanceof Error) {
                                            throw res[0][0];
                                        }
                                        // console.log(duration);
                                        return channel_1.sendTransaction({
                                            proposalResponses: res[0],
                                            proposal: res[1]
                                        }, TIMEOUT);
                                    }).then(function (res) {
                                        if (res.status !== "SUCCESS") {
                                            console.error("Thread " + TASK_ID + ": " + res.status);
                                        }
                                        return res;
                                    })["catch"](function (e) { return __awaiter(_this, void 0, void 0, function () {
                                        return __generator(this, function (_a) {
                                            console.log("<" + TASK_ID + "> Power-Fabric" + e);
                                            if (--errorLife_1 < 0) {
                                                throw Error("<" + TASK_ID + "> \uC624\uB958 \uC81C\uD55C \uD69F\uC218 \uCD08\uACFC");
                                            }
                                            return [2 /*return*/, null];
                                        });
                                    }); })) >= BATCH)) return [3 /*break*/, 9];
                                    // console.log(`bcResps length: ${bcResps.length}`)
                                    _a = count;
                                    return [4 /*yield*/, Promise.all(bcResps)];
                                case 6:
                                    // console.log(`bcResps length: ${bcResps.length}`)
                                    count = _a + (_b.sent()).length;
                                    if (!(count > nextCount)) return [3 /*break*/, 8];
                                    return [4 /*yield*/, sleep(0)];
                                case 7:
                                    _b.sent();
                                    // console.log("[tps: ", GOAL_TPS_CUR_THREAD * NUM_THREAD * NUM_CLIENT, "] Request Count: ", requestCount)
                                    // console.log(`<${TASK_ID}> #${nextCount} 돌/파 (${INPUT.length} / ${INPUT_LENGTH} 덩어리)`);
                                    nextCount += STEP;
                                    _b.label = 8;
                                case 8:
                                    bcResps = [];
                                    _b.label = 9;
                                case 9: return [2 /*return*/];
                            }
                        });
                    };
                    _b.label = 6;
                case 6:
                    if (!true) return [3 /*break*/, 8];
                    return [5 /*yield**/, _loop_1()];
                case 7:
                    state_1 = _b.sent();
                    if (state_1 === "break")
                        return [3 /*break*/, 8];
                    return [3 /*break*/, 6];
                case 8:
                    _a = count;
                    return [4 /*yield*/, Promise.all(bcResps)];
                case 9:
                    count = _a + (_b.sent()).length;
                    gateway.disconnect();
                    // console.log(`<${TASK_ID}> 처리 완료. ${count}개를 개당 평균 ${duration.toFixed(1)}ms만에 처리함)`);
                    return [4 /*yield*/, SET("benchmark-result-" + TASK_ID + "-n", count)];
                case 10:
                    // console.log(`<${TASK_ID}> 처리 완료. ${count}개를 개당 평균 ${duration.toFixed(1)}ms만에 처리함)`);
                    _b.sent();
                    // await SET(`benchmark-result-${TASK_ID}-t`, duration);
                    process.exit(0);
                    return [3 /*break*/, 12];
                case 11:
                    error_1 = _b.sent();
                    console.error("Thread " + TASK_ID + ": Failed to submit transaction: " + error_1);
                    console.error(error_1 === null || error_1 === void 0 ? void 0 : error_1.stack);
                    process.exit(1);
                    return [3 /*break*/, 12];
                case 12: return [2 /*return*/];
            }
        });
    });
}
main();
function sleep(second) {
    if (second <= 0) {
        return new Promise(function (res) { return res(); });
    }
    var now = Date.now();
    var q = (1 + Math.floor(now / second)) * second;
    return new Promise(function (res) {
        global.setTimeout(res, q - now);
    });
}
function getRandomArbitrary(min, max) {
    return Math.random() * (max - min) + min;
}
function sleep_ms(ms) {
    return new Promise(function (resolve) { return setTimeout(resolve, ms); });
}
