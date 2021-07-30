// This test is meant to be spawned from idle-timeout.js
if (module === require.main) {
  const allowExitOnIdle = process.env.ALLOW_EXIT_ON_IDLE === '1'
  const Pool = require('../index')

  const pool = new Pool({ idleTimeoutMillis: 200, ...(allowExitOnIdle ? { allowExitOnIdle: true } : {}) })
  pool.query('SELECT NOW()', (err, res) => console.log('completed first'))
  pool.on('remove', () => {
    console.log('removed')
    done()
  })

  setTimeout(() => {
    pool.query('SELECT * from generate_series(0, 1000)', (err, res) => console.log('completed second'))
  }, 50)
}
