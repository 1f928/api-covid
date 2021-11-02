
const express = require('express');
const router = express.Router();

router.get('/', (req, res) => {
  res.send('County route response!');
});

module.exports = router;
