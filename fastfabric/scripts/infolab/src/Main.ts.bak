/*
 * SPDX-License-Identifier: Apache-2.0
 */

'use strict';

import { FileSystemWallet, Gateway } from 'fabric-network';
import Client = require("../node_modules/fabric-client/types");
import ChildProcess = require("child_process");
import FS = require("fs");
import Path = require("path");
import Redis = require("redis");
import { promisify } from "util";

let avg_send_tps = 0
let total_send_tps = 0
let send_tps_count = 0

let BATCH = 1
const STEP = 100

const NUM_TPS = BATCH

let NUM_THREAD = 40
const NUM_CLIENT = 15



const NUM_SUN = 9
const NUM_APOLLO = 5
const SUN_THREAD = 40
const APOLLO_THREAD = 64

const GOAL_TPS = 16000
const GOAL_TPS_CUR_THREAD = GOAL_TPS / NUM_THREAD / NUM_CLIENT

// const GOAL_TPS_CUR_THREAD = GOAL_TPS / (SUN_THREAD * NUM_SUN + APOLLO_THREAD * NUM_APOLLO)

// const GOAL_DURATION = Math.round(NUM_TPS / GOAL_TPS_CUR_THREAD * 1000)
const GOAL_DURATION = NUM_TPS / GOAL_TPS_CUR_THREAD * 1000 // Num_TPS가 몇ms에 한번씩 처리가 되어야 하나

// const BATCH = 3;
// const STEP = 2000;

// How many writes per 20 transactions // 
const Pw = 19



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
const TIMEOUT = 5000 * 10000;
const redis = Redis.createClient({
  host: "master.jjo.kr",
  port: 16379
});
const SET = promisify(redis.set).bind(redis);
let INPUT_LENGTH:number;

function readFile(path:string):Promise<string[]>{
  const stream = FS.createReadStream(path);
  let R = [];

  return new Promise(res => {
    stream.on('data', chunk => {
      R.push(chunk.toString());
    });
    stream.on('end', () => {
      stream.close();
      // R = [ R.join('') ];
      INPUT_LENGTH = R.length;
      console.log(process.argv[2], "읽기 완료", INPUT_LENGTH);
      res(R);
    });
  });
}
async function main(){
  const TASK_ID = parseInt(process.argv[2]);
  const TASK_COUNT = parseInt(process.argv[3]);
  const INPUT = await readFile(process.argv[4]);
  const METHOD = process.argv[5]; // send2, send4, send6, sendx
  // let METHOD = "send8"; // send2, send4, send6, sendx

  let maxConcat:number;
  let concat = 0;
  let requestHandler:(chunk:string[]) => { 'fcn': string, 'args': string[] };
  let index = 0;

  // await sleep_ms(getRandomArbitrary(1, 1000));
  // await sleep_ms( (1000 / NUM_THREAD) *( TASK_ID % NUM_THREAD) + TASK_ID / NUM_CLIENT)


  switch(METHOD){
    default:
    case "send2":
      maxConcat = 1;
      requestHandler = chunk => ({
        fcn: METHOD,
        args: [ chunk[0], chunk[1], chunk[2] ]
      });
      break;
    case "send4":
      maxConcat = 2;
      requestHandler = chunk => ({
        fcn: METHOD,
        args: [ chunk[0], chunk[1], chunk[3], chunk[4], chunk[5] ]
      });
      break;
    case "send6":
      maxConcat = 3;
      requestHandler = chunk => ({
        fcn: METHOD,
        args: [ chunk[0], chunk[1], chunk[3], chunk[4], chunk[6], chunk[7], chunk[8] ]
      });
      break;
    case "send8":
      maxConcat = 4;
      requestHandler = chunk => ({
        fcn: METHOD,
        args: [ chunk[0], chunk[1], chunk[3], chunk[4], chunk[6], chunk[7], chunk[9], chunk[10], chunk[11] ]
      });
      break;
    case "readwrite4":
      maxConcat = 4;
      requestHandler = chunk => ({
        fcn: METHOD,
        args: [ chunk[0], chunk[1], chunk[3], chunk[4], chunk[6], chunk[7], chunk[9], chunk[10] ]
      });
      break;
    case "readwrite8":
      maxConcat = 8;
      requestHandler = chunk => ({
        fcn: METHOD,
        args: [ chunk[0], chunk[1], chunk[3], chunk[4], chunk[6], chunk[7], chunk[9], chunk[10], chunk[12], chunk[13], chunk[15], chunk[16], chunk[18], chunk[19], chunk[21], chunk[22] ]
      });
      break;
    case "fabric++":
      maxConcat = 1;
      requestHandler = () => {
        const R = {
          fcn: METHOD,
          args: [ String(index) ]
        };
        index += 8;

        return R;
      };
      break;
  }

  var bcResps: Array<Promise<Client.BroadcastResponse>> = [];

  var ccpPath = Path.resolve(__dirname, 'connection.json');
  const ccpJSON = FS.readFileSync(ccpPath, 'utf8');
  const ccp = JSON.parse(ccpJSON);

  // var endorserAddresses = [ "fast.org1.jjo.kr" ];
  var endorserAddresses = ChildProcess.execSync("bash printEndorsers.sh").toString().replace("\n","").split(" ")
  // console.log(`Endorser address: "${ccp.peers.address.url}"`);
  // endorserAddresses.push("fast.org1.jjo.kr");

  ccp.name = ccp.name.replace("CHANNEL", process.env.CHANNEL)
  ccp.organizations.Org1.signedCert.path = ccp.organizations.Org1.signedCert.path.split("DOMAIN").join(process.env.PEER_DOMAIN)
  ccp.orderers.address.url = ccp.orderers.address.url.replace("ADDRESS", process.env.ORDERER_ADDRESS + "." + process.env.ORDERER_DOMAIN)
  ccp.peers.address.url = ccp.peers.address.url.replace("ADDRESS", endorserAddresses[TASK_ID % endorserAddresses.length]);
  console.log(`Endorser address: "${ccp.peers.address.url}"`);

  const user = "Admin@" + process.env.PEER_DOMAIN;

  console.log(`Thread no.: ${TASK_ID} -> ${ccp.peers.address.url}`);
  const wallet = new FileSystemWallet(Path.resolve(__dirname, "..", 'wallet'));
  try {

    // Check to see if we've already enrolled the user.
    const userExists = await wallet.exists(user);
    if (!userExists) {
      console.log(`An identity for the user "${user}" does not exist in the wallet`);
      console.log('Run the registerUser.js application before retrying');
      return;
    }

    // Create a new gateway for connecting to our peer node.
    var gateway = new Gateway();
    await gateway.connect(ccp, { wallet, identity: user, discovery: { enabled: false } });

    // Get the network (channel) our contract is deployed to.
    var network = await gateway.getNetwork(String(process.env.CHANNEL));
    const channel = network.getChannel();
    const client = gateway.getClient();
    const redisSet = promisify(redis.set).bind(redis);
    let pair = /^(\S+)\s+(\S+)(?:\s+([.-\d]+))?$/gm;
    let chunk:string[] = [];
    let count = 0;
    let nextCount = STEP;
    let errorLife = 5 * BATCH;
    let i = 0;
    let input = INPUT.shift();
    let length = input.length;

    let requestCount = 1;
    let writeCount = TASK_ID;
    let durationStartTime = Date.now();

    let lastDur = 0;
    // console.log("input is: ", input)
    // console.log("input end with length: ", INPUT.length)


    let prevExecIndex = 0
    let prevExecLength = 0


    
    while(true){
      if(length - prevExecIndex < 100 && INPUT.length){
        input = input.slice(prevExecIndex + prevExecLength) + INPUT.shift();
        length = input.length;
        pair = new RegExp(pair);
      }

      const exec = pair.exec(input);
      prevExecIndex = exec.index
      prevExecLength = exec[0].length
      let startedAt:number;
      
      if(!exec){
        break;
      }
      // if(length - exec.index < 100 && INPUT.length){
      //   input = input.slice(exec.index + exec[0].length) + INPUT.shift();
      //   length = input.length;
      //   pair = new RegExp(pair);
      // }
      // console.log("input length: ", input.length)
      i++;
      if(i % TASK_COUNT !== TASK_ID){
        continue;
      }
      chunk.push(exec[1], exec[2], String(Math.round(1e-8 * (exec[3] as any || 1e+8))));
      concat++;
      if(concat % maxConcat){
        continue;
      }

      // read or write
      let data = requestHandler(chunk);
      const txId = client.newTransactionID();

      chunk = [];
      concat = 0;
      startedAt = Date.now();
      
      // console.log("args: ", data.args)
      let curTps = 0
      if (requestCount > 0 && requestCount % NUM_TPS == 0) {
        let curDuration = Date.now() - durationStartTime
        let remainderTime = (GOAL_DURATION * requestCount / NUM_TPS) - curDuration
        // console.log("TASK_ID: ", TASK_ID)
        if (remainderTime > 0) {
          // console.log(TASK_ID, " CUR TPS: ", requestCount / (curDuration / 1000))
          // console.log("Sleeping for ", remainderTime)

          await new Promise(resolve => setTimeout(resolve, remainderTime));
          curDuration = Date.now() - durationStartTime
          curTps = requestCount / (curDuration / 1000) * NUM_CLIENT * NUM_THREAD
        }
        else {
          curTps = requestCount / (curDuration / 1000) * NUM_CLIENT * NUM_THREAD
          // console.log("TPS is too slow with ", requestCount / (curDuration / 1000) * NUM_CLIENT * NUM_THREAD)
        }
        total_send_tps += curTps
        send_tps_count++
        if (send_tps_count % 100 == 0) {
          avg_send_tps = total_send_tps / send_tps_count
          console.log(`${TASK_ID}\t${curTps}`)
          console.log(`requestCount: ${requestCount}\tcurDuration:${curDuration}\tdiff:${curDuration - lastDur}`)
          lastDur = curDuration

        }

      }
      requestCount++
      await redisSet(`ptst-${txId.getTransactionID()}`, String(startedAt));

      if(bcResps.push(channel.sendTransactionProposal({
        chaincodeId: String(process.env.CHAINCODE),
        ...data,
        // args: [ String(2 * i), String(2 * i + 1), "1" ],
        // args: [ String(2 * i), String(2 * i + 1), chunk[2] ],
        txId
      }, TIMEOUT).then(res => {
        if(res[0]?.[0] instanceof Error){
          throw res[0][0];
        }
        // console.log(duration);

        return channel.sendTransaction({
          proposalResponses: res[0] as Client.ProposalResponse[],
          proposal: res[1]
        }, TIMEOUT);
      }).then(res => {
        if(res.status !== "SUCCESS"){
          console.error(`Thread ${TASK_ID}: ${res.status}`);
        }
        return res;
      }).catch(async e => {
        console.log(`<${TASK_ID}> Power-Fabric${e}`);
        if(--errorLife < 0){
          throw Error(`<${TASK_ID}> 오류 제한 횟수 초과`);
        }
        return null;
      })) >= BATCH){
        // console.log(`bcResps length: ${bcResps.length}`)
        count += (await Promise.all(bcResps)).length;
        if(count > nextCount){
          await sleep(0);
          // console.log("[tps: ", GOAL_TPS_CUR_THREAD * NUM_THREAD * NUM_CLIENT, "] Request Count: ", requestCount)
          // console.log(`<${TASK_ID}> #${nextCount} 돌/파 (${INPUT.length} / ${INPUT_LENGTH} 덩어리)`);
          nextCount += STEP;
        }
        bcResps = [];
      }
    }
    
    count += (await Promise.all(bcResps)).length;
    gateway.disconnect();

    // console.log(`<${TASK_ID}> 처리 완료. ${count}개를 개당 평균 ${duration.toFixed(1)}ms만에 처리함)`);
    await SET(`benchmark-result-${TASK_ID}-n`, count);
    // await SET(`benchmark-result-${TASK_ID}-t`, duration);
    process.exit(0);
  } catch (error) {
    console.error(`Thread ${TASK_ID}: Failed to submit transaction: ${error}`);
    console.error(error?.stack);
    process.exit(1);
  }
}

main();

function sleep(second:number):Promise<void>{
  if(second <= 0){
    return new Promise(res => res());
  }
  const now = Date.now();
  const q = (1 + Math.floor(now / second)) * second;
  
  return new Promise(res => {
    global.setTimeout(res, q - now);
  });
}

function getRandomArbitrary(min, max) {
  return Math.random() * (max - min) + min;
}


function sleep_ms(ms: number) {
  return new Promise<number>( resolve => setTimeout(resolve, ms) );
}
