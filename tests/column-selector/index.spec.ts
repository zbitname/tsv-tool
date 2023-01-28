import {
    writeFileSync,
    rmSync,
} from 'fs';
import { ColumnSelectorTransform } from '../../src';

describe('Column Selector', () => {
    describe('Simple small input file', () => {
        const tmpFile = '/tmp/cs-bc-1.tsv';
        const inputData = [
            ['col1', 'col2', 'col3', 'col4'].join('\t'),
            ['1234', '2345', '3456', '4567'].join('\t'),
            ['2345', '3456', '4567', '5678'].join('\t'),
            ['3456', '4567', '5678', '6789'].join('\t'),
        ].join('\n');

        before(async () => {
            writeFileSync(tmpFile, inputData);
        });

        after(() => {
            rmSync(tmpFile);
        });

        it('Select 1st column', (cb) => {
            const selector = new ColumnSelectorTransform({}, {
                colIndexes: [1],
            })
        });
    });
});
