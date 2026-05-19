const fs = require('fs');
const http = require('http');
const https = require('https');
const { EventEmitter } = require('events');
const express = require('express');
const bodyParser = require('body-parser');
const { createDecipher } = require('crypto');
const { connect } = require('ngrok');
require('dotenv').config();
const _ = require('lodash');
const logger = require('./logger');
const {
  hangupCall, bridgeCall, acceptCall, startRecording, stopRecording,
} = require('./voiceapi');

const app = express();
const eventEmitter = new EventEmitter();

let server;
let webHookUrl;
const call = {};
const sseMsg = [];
const servicePort = process.env.SERVICE_PORT || 3000;

function onError(error) {
  if (error.syscall !== 'listen') {
    throw error;
  }
  switch (error.code) {
    case 'EACCES':
      logger.error(`Port ${servicePort} requires elevated privileges`);
      process.exit(1);
      break;
    case 'EADDRINUSE':
      logger.error(`Port ${servicePort} is already in use`);
      process.exit(1);
      break;
    default:
      throw error;
  }
}

function shutdown() {
  server.close(() => {
    logger.error('Shutting down the server');
    process.exit(0);
  });
  setTimeout(() => {
    process.exit(1);
  }, 10000);
}

function createNgrokTunnel() {
  server = app.listen(servicePort, () => {
    logger.info(`Server running on port ${servicePort}`);
    (async () => {
      try {
        webHookUrl = process.env.PUBLIC_WEBHOOK_URL;
        logger.info(`Ngrok tunnel set up: ${webHookUrl}`);
      } catch (error) {
        logger.error(`Error connecting via ngrok: ${JSON.stringify(error)}`);
        shutdown();
        return;
      }
      webHookUrl += '/event';
      logger.info(`Webhook URL for inbound calls: ${webHookUrl}`);
    })();
  });
}

function setWebHookEventUrl() {
  logger.info(`Listening on port ${servicePort}`);
  webHookUrl = `${process.env.PUBLIC_WEBHOOK_HOST}/event`;
  logger.info(`Webhook URL for inbound calls: ${webHookUrl}`);
}

function createAppServer() {
  if (process.env.LISTEN_SSL) {
    const options = {
      key: fs.readFileSync(process.env.CERTIFICATE_SSL_KEY).toString(),
      cert: fs.readFileSync(process.env.CERTIFICATE_SSL_CERT).toString(),
    };
    if (process.env.CERTIFICATE_SSL_CACERTS) {
      options.ca = [];
      options.ca.push(fs.readFileSync(process.env.CERTIFICATE_SSL_CACERTS).toString());
    }
    server = https.createServer(options, app);
  } else {
    server = http.createServer(app);
  }
  app.set('port', servicePort);
  server.listen(servicePort);
  server.on('error', onError);
  server.on('listening', setWebHookEventUrl);
}

if (process.env.ENABLEX_APP_ID && process.env.ENABLEX_APP_KEY) {
  if (process.env.USE_NGROK_TUNNEL === 'true' && process.env.USE_PUBLIC_WEBHOOK === 'false') {
    createNgrokTunnel();
  } else if (process.env.USE_PUBLIC_WEBHOOK === 'true' && process.env.USE_NGROK_TUNNEL === 'false') {
    createAppServer();
  } else {
    logger.error('Incorrect configuration - set either USE_NGROK_TUNNEL or USE_PUBLIC_WEBHOOK to true (not both)');
  }
} else {
  logger.error('Missing required env variables: ENABLEX_APP_ID, ENABLEX_APP_KEY');
}

process.on('SIGINT', () => {
  logger.info('Caught interrupt signal');
  shutdown();
});

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(express.static('client'));

app.get('/event-stream', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
  });

  const id = (new Date()).toLocaleTimeString();

  setInterval(() => {
    if (!_.isEmpty(sseMsg[0])) {
      const data = `${sseMsg[0]}`;
      res.write(`id: ${id}\n`);
      res.write(`data: ${data}\n\n`);
      sseMsg.shift();
    }
  }, 100);
});

app.post('/event', (req, res) => {
  let jsonObj;
  if (req.headers['x-algoritm'] !== undefined) {
    // EnableX sends 'x-algoritm' (their spelling) for encrypted webhook payloads
    const key = createDecipher(req.headers['x-algoritm'], process.env.ENABLEX_APP_ID);
    let decryptedData = key.update(req.body.encrypted_data, req.headers['x-format'], req.headers['x-encoding']);
    decryptedData += key.final(req.headers['x-encoding']);
    jsonObj = JSON.parse(decryptedData);
  } else {
    jsonObj = req.body;
  }
  logger.info(`Webhook event: ${JSON.stringify(jsonObj)}`);
  res.status(200).send();
  sseMsg.push('__WEBHOOK__:' + JSON.stringify(jsonObj));
  eventEmitter.emit('voicestateevent', jsonObj);
});

function timeOutHandler(voiceId) {
  logger.info(`[${voiceId}] Disconnecting the call`);
  hangupCall(voiceId, () => {});
}

function recordingStop(voiceId) {
  logger.info(`[${voiceId}] Stopping recording`);
  stopRecording(voiceId, () => {});
}

function recordingStart(voiceId) {
  logger.info(`[${voiceId}] Starting recording`);
  startRecording(voiceId, 'bridgerecording_inbound_03', () => {});
}

function pushEvent(state, message) {
  sseMsg.push(JSON.stringify({ state, message, timestamp: new Date().toLocaleTimeString() }));
}

function voiceEventHandler(voiceEvent) {
  const voiceId = voiceEvent.voice_id;

  if (voiceEvent.state === undefined) return;

  if (voiceEvent.state === 'incomingcall') {
    logger.info(`[${voiceId}] Received an inbound call`);
    pushEvent('incomingcall', `[${voiceId}] Received an inbound call`);
    setTimeout(() => { acceptCall(voiceId, () => {}); }, 1000);
  } else if (voiceEvent.state === 'connected') {
    logger.info(`[${voiceId}] Call connected`);
    pushEvent('connected', `[${voiceId}] Call connected`);
    setTimeout(() => { bridgeCall(voiceId, process.env.FROM, process.env.BRIDGETO, () => {}); }, 1000);
  } else if (voiceEvent.state === 'bridged') {
    logger.info(`[${voiceId}] Call bridged`);
    pushEvent('bridged', `[${voiceId}] Call bridged to ${process.env.BRIDGETO}`);
    setTimeout(recordingStart, 1000, voiceId);
    setTimeout(recordingStop, 12000, voiceId);
    setTimeout(timeOutHandler, 30000, voiceId);
  } else if (voiceEvent.state === 'bridge_disconnected') {
    logger.info(`[${voiceId}] Bridge disconnected`);
    pushEvent('bridge_disconnected', `[${voiceId}] Bridge disconnected`);
  } else if (voiceEvent.state === 'disconnected') {
    logger.info(`[${voiceId}] Call disconnected`);
    pushEvent('disconnected', `[${voiceId}] Call disconnected`);
  }
}

eventEmitter.on('voicestateevent', voiceEventHandler);
