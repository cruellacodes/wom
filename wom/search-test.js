import http from 'k6/http';
import { check, sleep } from 'k6';

export let options = {
  vus: 200,           // Simulate 200 concurrent users
  duration: '1m',     // for 1 minute
};

export default function () {
  const symbols = ['doge', 'bonk', 'pepe', 'chat', 'sol', 'wif'];  // rotate tokens
  const token = symbols[Math.floor(Math.random() * symbols.length)];
  const res = http.get(`https://word-of-mouth-xojl.onrender.com/tweets/${token}`);

  check(res, {
    'status is 200': (r) => r.status === 200,
    'has wom_score': (r) => r.body.includes('wom_score'),
  });

  sleep(1); // simulate user delay
}
