import fs from 'fs';
import { Transform, TransformCallback, TransformOptions } from 'stream';

const COLUMN_DELIMITER = Buffer.from('\t');
const ROW_DELIMITER = Buffer.from('\n');
const COLUMN_DELIMITER_UINT8 = COLUMN_DELIMITER.readUInt8(0);
const ROW_DELIMITER_UINT8 = ROW_DELIMITER.readUInt8(0);

export class ColumnSelectorTransform extends Transform {
    private prevBuffer = Buffer.alloc(0);
    private maxColIdx: number;

    constructor(
        opts: TransformOptions,
        private params: {
            colIndexes: number[];
        },
    ) {
        super(opts);
        this.maxColIdx = Math.max(...this.params.colIndexes);
    }

    _transform(chunk: any, encoding: BufferEncoding, callback: TransformCallback): void {
        const buf = Buffer.concat([this.prevBuffer, chunk]);
        const cache = Buffer.alloc(buf.byteLength * 2);

        let startRowPosition = 0;
        let finishRowPosition = buf.indexOf(ROW_DELIMITER_UINT8, startRowPosition);
        let colNumber = 0;
        let cacheOffset = 0;

        while (finishRowPosition >= 0) {
            const row = buf.subarray(startRowPosition, finishRowPosition);
            let startColPosition = 0;
            let finishColPosition = 0;

            colNumber = 0;
            finishColPosition = row.indexOf(COLUMN_DELIMITER_UINT8, startColPosition);

            while ((colNumber <= this.maxColIdx) || (finishColPosition < finishRowPosition && finishColPosition >= 0)) {
                if (this.params.colIndexes.indexOf(colNumber) >= 0) {
                    const v = row.subarray(startColPosition, finishColPosition);
                    const l = v.byteLength;

                    cache.fill(v, cacheOffset, l + cacheOffset);
                    cacheOffset += l;

                    if (colNumber === this.maxColIdx) {
                        cache.fill(ROW_DELIMITER_UINT8, cacheOffset, cacheOffset + 1);
                        cacheOffset++;
                    } else {
                        cache.fill(COLUMN_DELIMITER_UINT8, cacheOffset, cacheOffset + 1);
                        cacheOffset++;
                    }
                }

                startColPosition = finishColPosition + 1;
                colNumber++;
                finishColPosition = row.indexOf(COLUMN_DELIMITER_UINT8, startColPosition);

                if (finishColPosition === -1) {
                    finishColPosition = finishRowPosition;
                }
            }

            startRowPosition = finishRowPosition + 1;
            finishRowPosition = buf.indexOf(ROW_DELIMITER_UINT8, startRowPosition);
        }

        this.prevBuffer = buf.subarray(startRowPosition);

        callback(null, cache.subarray(0, cacheOffset));
    }
}
