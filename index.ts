import task from 'tasuku';

function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

await task.group((task) => [
  task('default', async ({ setTitle }) => {
    await sleep(1000);
    setTitle('default successfully finished!');
  }),

  task('task1', async ({ setTitle, setError }) => {
    await sleep(1000);
    try {
      throw('xxx error');
    } catch (error) {
      setError('task1 failed: ' + error);
    }
    setTitle('task1 successfully finished!');
  }),

  task('task2', async ({ setTitle }) => {
    await sleep(1000);
    setTitle('task2 successfully finished!');
  }),

  task('task3', async ({ setTitle }) => {
    await sleep(1000);
    setTitle('task3 successfully finished!');
  }),
]);