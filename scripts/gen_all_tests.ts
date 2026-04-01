const fs = require('fs');
const path = require('path');
const glob = require('glob');

function genAllTests() {
  const files = glob.sync('tests/**/*.ts');
  const content = files
    .filter(
      (f) =>
        !f.endsWith('selenium_runner.ts') &&
        !f.includes('tests/harness/') &&
        !f.endsWith('all_tests.ts')
    )
    .map((f) => {
      // Use relative path for imports
      const relativePath = path.relative('tests', f).replace(/\\/g, '/');
      return `import './${relativePath.replace('.ts', '')}';`;
    })
    .join('\n');
  fs.writeFileSync('tests/all_tests.ts', content, 'utf-8');
  console.log('Generated tests/all_tests.ts');
}

genAllTests();
