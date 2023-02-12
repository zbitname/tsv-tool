/* eslint-env mocha */

import {
    writeFileSync,
    rmSync,
    createReadStream,
} from 'fs';

import { expect } from 'chai';

import { ColumnSelectorTransform } from '../../src';


describe('Column Selector', () => {
    const tests = [
        {
            name: 'Same cell sizes, 3x2, with blank line on the end',
            inputData: [
                ['col1', 'col2', 'col3'],
                ['1234', '2345', '3456'],
                [], // trailing empty string
            ],
        },
        {
            name: 'Different cell sizes, 3x2, with blank line on the end',
            inputData: [
                ['col1', 'col2', 'col3'],
                ['1', '23', '345'],
                [], // trailing empty string
            ],
        },
        {
            name: 'Same cell sizes, 3x6, with blank line on the end',
            inputData: [
                ['col1', 'col2', 'col3'],
                ['1234', '2345', '3456'],
                ['2345', '3456', '4567'],
                ['3456', '4567', '5678'],
                ['4567', '5678', '6789'],
                ['5678', '6789', '7890'],
                [], // trailing empty string
            ],
        },
        {
            name: 'Same cell sizes, 4x6, with blank line on the end and blank columns',
            inputData: [
                ['col1', 'col2', 'col3', 'col4'],
                ['1234', '2345', '', '4567'],
                ['2345', '3456', '', '5678'],
                ['3456', '4567', '', '6789'],
                ['4567', '5678', '', '7890'],
                ['5678', '6789', '', '8901'],
                [], // trailing empty string
            ],
        },
        {
            name: 'Same cell sizes, 3x2, without blank line on the end',
            inputData: [
                ['col1', 'col2', 'col3'],
                ['1234', '2345', '3456'],
            ],
        },
        {
            name: 'Different cell sizes, 3x2, without blank line on the end',
            inputData: [
                ['col1', 'col2', 'col3'],
                ['1', '23', '345'],
            ],
        },
        {
            name: 'Same cell sizes, 3x6, without blank line on the end',
            inputData: [
                ['col1', 'col2', 'col3'],
                ['1234', '2345', '3456'],
                ['2345', '3456', '4567'],
                ['3456', '4567', '5678'],
                ['4567', '5678', '6789'],
                ['5678', '6789', '7890'],
            ],
        },
        {
            name: 'Same cell sizes, 4x6, without blank line on the end and blank columns',
            inputData: [
                ['col1', 'col2', 'col3', 'col4'],
                ['1234', '2345', '', '4567'],
                ['2345', '3456', '', '5678'],
                ['3456', '4567', '', '6789'],
                ['4567', '5678', '', '7890'],
                ['5678', '6789', '', '8901'],
            ],
        },
    ];

    for (const chunkSize of [1, 5, 10, 20, 100, 1000]) {
        describe(`File / Selection by index / CHUNK_SIZE=${chunkSize}`, () => {
            for (const test of tests) {
                const tmpFile = `/tmp/${Math.random() * 10000000}.tsv`;
                const { name, inputData } = test;

                before(async () => {
                    writeFileSync(tmpFile, inputData.map(i => i.join('\t')).join('\n'));
                });

                after(() => {
                    rmSync(tmpFile);
                });

                for (let colIdx = 0; colIdx < inputData[0].length; colIdx++) {
                    it(`${name}. Select column #${colIdx}`, (cb) => {
                        let result = '';
                        const selector = new ColumnSelectorTransform({}, {
                            colIndexes: [colIdx],
                        });

                        const r = createReadStream(tmpFile).pipe(selector);

                        r.on('data', (chunk) => {
                            result += chunk.toString();
                        });

                        r.once('end', () => {
                            expect(result).eq(inputData.map(i => i[colIdx]).join('\n'));
                            cb();
                        });
                    });
                }

                for (let colIdx1 = 0; colIdx1 < inputData[0].length - 1; colIdx1++) {
                    for (let colIdx2 = colIdx1 + 1; colIdx2 < inputData[0].length; colIdx2++) {
                        it(`${name}. Select columns #{${colIdx1}, ${colIdx2}}`, (cb) => {
                            let result = '';
                            const selector = new ColumnSelectorTransform({}, {
                                colIndexes: [colIdx1, colIdx2],
                            });

                            const r = createReadStream(tmpFile).pipe(selector);

                            r.on('data', (chunk) => {
                                result += chunk.toString();
                            });

                            r.once('end', () => {
                                expect(result).eq(inputData.map(
                                    i => [i[colIdx1], i[colIdx2]].filter(i => typeof i !== 'undefined').join('\t')
                                ).join('\n'));
                                cb();
                            });
                        });
                    }
                }
            }
        });

        describe(`File / Selection by column name / CHUNK_SIZE=${chunkSize}`, () => {
            for (const test of tests) {
                const tmpFile = `/tmp/${Math.random() * 10000000}.tsv`;
                const { name, inputData } = test;

                before(async () => {
                    writeFileSync(tmpFile, inputData.map(i => i.join('\t')).join('\n'));
                });

                after(() => {
                    rmSync(tmpFile);
                });

                for (let colIdx = 0; colIdx < inputData[0].length; colIdx++) {
                    it(`${name}. Select column #${colIdx}(${test.inputData[0][colIdx]})`, (cb) => {
                        let result = '';
                        const selector = new ColumnSelectorTransform({}, {
                            colNames: [test.inputData[0][colIdx]],
                        });

                        const r = createReadStream(tmpFile).pipe(selector);

                        r.on('data', (chunk) => {
                            result += chunk.toString();
                        });

                        r.once('end', () => {
                            expect(result).eq(inputData.map(i => i[colIdx]).join('\n'));
                            cb();
                        });
                    });
                }

                for (let colIdx1 = 0; colIdx1 < inputData[0].length - 1; colIdx1++) {
                    for (let colIdx2 = colIdx1 + 1; colIdx2 < inputData[0].length; colIdx2++) {
                        it(`${name}. Select columns #{${colIdx1}(${test.inputData[0][colIdx1]}), ${colIdx2}(${test.inputData[0][colIdx2]})}`, (cb) => {
                            let result = '';
                            const selector = new ColumnSelectorTransform({}, {
                                colNames: [test.inputData[0][colIdx1], test.inputData[0][colIdx2]],
                            });

                            const r = createReadStream(tmpFile).pipe(selector);

                            r.on('data', (chunk) => {
                                result += chunk.toString();
                            });

                            r.once('end', () => {
                                expect(result).eq(inputData.map(
                                    i => [i[colIdx1], i[colIdx2]].filter(i => typeof i !== 'undefined').join('\t')
                                ).join('\n'));
                                cb();
                            });
                        });
                    }
                }
            }
        });
    }
});
