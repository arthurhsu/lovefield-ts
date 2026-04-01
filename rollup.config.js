import typescript from '@rollup/plugin-typescript';
import resolve from '@rollup/plugin-node-resolve';
import commonjs from '@rollup/plugin-commonjs';

export default [
  {
    input: 'lib/index.ts',
    output: [
      {
        file: 'dist/lovefield.js',
        format: 'cjs',
        sourcemap: true,
      },
      {
        file: 'dist/lovefield.esm.js',
        format: 'es',
        sourcemap: true,
      },
      {
        file: 'dist/lovefield.umd.js',
        format: 'umd',
        name: 'lf',
        sourcemap: true,
      },
    ],
    plugins: [
      resolve(),
      commonjs(),
      typescript({
        tsconfig: './tsconfig.json',
        compilerOptions: {
          module: 'ESNext',
          outDir: 'dist',
          declaration: true,
          declarationDir: 'dist',
        },
        rootDir: 'lib',
      }),
    ],
    onwarn(warning, warn) {
      if (warning.code === 'CIRCULAR_DEPENDENCY') return;
      warn(warning);
    },
  },
];
