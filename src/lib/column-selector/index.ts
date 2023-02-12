import { Transform, TransformCallback, TransformOptions } from 'stream';

const COL_DELIMITER_STR = '\t';
const COL_DELIMITER = Buffer.from(COL_DELIMITER_STR);
const ROW_DELIMITER = Buffer.from('\n');
const COL_DELIMITER_UINT8 = COL_DELIMITER.readUInt8(0);
const ROW_DELIMITER_UINT8 = ROW_DELIMITER.readUInt8(0);

export class ColumnSelectorTransform extends Transform {
    private prevBuffer = Buffer.alloc(0);
    private maxColIdx!: number;
    private needColIdxs!: number[];
    private needHeader = true;
    private cacheBufferMultiplier = 1;

    constructor(
        opts: TransformOptions,
        private params: {
            colIndexes?: number[];
            colNames?: string[];
            cacheBufferMultiplier?: number;
        },
    ) {
        super(opts);

        if (params.cacheBufferMultiplier && params.cacheBufferMultiplier < 1) {
            throw new Error('cacheBufferMultiplier can not be less than 1');
        }

        if (params.colIndexes) {
            this.needColIdxs = params.colIndexes.sort();
            this.maxColIdx = this.needColIdxs[this.needColIdxs.length - 1];
            this.needHeader = false;
        }
    }

    // eslint-disable-next-line no-undef
    _transform(chunk: any, encoding: BufferEncoding, callback: TransformCallback): void {
        if (this.needHeader) {
            this.__parseHeader(chunk);

            if (this.needHeader) {
                callback(null, Buffer.alloc(0));
            } else {
                callback(null, this.__parse(Buffer.alloc(0)));
            }
        } else {
            callback(null, this.__parse(chunk));
        }
    }

    _flush(callback: TransformCallback): void {
        if (this.prevBuffer.length > 0) {
            if (this.needHeader) {
                this.__parseHeader(Buffer.alloc(0), true);
            }
            callback(null, this.__parse(this.prevBuffer, true));

        } else {
            callback(null);
        }
    }

    __parseHeader(chunk: any, isFinish = false) {
        if (this.params.colNames) {
            const buf = isFinish ? chunk : Buffer.concat([this.prevBuffer, chunk]);
            const rowDelimiterPos = buf.indexOf(ROW_DELIMITER_UINT8);

            if (rowDelimiterPos >= 0 || isFinish) {
                const row = buf.subarray(0, rowDelimiterPos).toString().split(COL_DELIMITER_STR);
                const colNames = this.params.colNames;
                this.needColIdxs = row
                    .map((v: string, i: number) => colNames.indexOf(v) >= 0 ? i : -1)
                    .filter((v: number) => v >= 0)
                    .sort();
                this.needHeader = false;
                this.maxColIdx = this.needColIdxs[this.needColIdxs.length - 1];

                if (this.needColIdxs.length === 0) {
                    throw new Error('Nothing to select');
                }
            }
            this.prevBuffer = buf.slice();
            return;
        }

        throw new Error('You need to choose something (colIndexes or colNames) for column selection');
    }

    __parse(chunk: any, isFinish = false) {
        const buf = isFinish ? chunk : Buffer.concat([this.prevBuffer, chunk]);
        const result = Buffer.alloc(buf.byteLength * Math.round(this.cacheBufferMultiplier || 1) || 1);
        const maxColIdx = this.maxColIdx;

        let resultOffset = 0;
        let needCols = this.needColIdxs;
        let col = 0;
        let cellBeginPos = -1;
        let cellEndPos = -1;
        let nextCellDelimiterPos = -1;
        let rowDelimiterPos = buf.indexOf(ROW_DELIMITER_UINT8);
        let nextRowDelimiterPos = buf.indexOf(ROW_DELIMITER_UINT8, rowDelimiterPos + 1);
        let rowDelimiterPosForPrevBuffer = rowDelimiterPos;
        let endOfRow = false;
        let hasData = true;

        while (hasData) {
            if (rowDelimiterPos >= 0) {
                rowDelimiterPosForPrevBuffer = rowDelimiterPos;
            } else if (isFinish) {
                rowDelimiterPosForPrevBuffer = buf.length;
                rowDelimiterPos = rowDelimiterPosForPrevBuffer;
            } else {
                break;
            }

            cellBeginPos = cellEndPos + 1;
            nextCellDelimiterPos = buf.indexOf(COL_DELIMITER_UINT8, cellBeginPos);

            if (nextCellDelimiterPos <= rowDelimiterPos || isFinish) {
                if (isFinish && nextCellDelimiterPos < 0) {
                    if (rowDelimiterPos >= 0) {
                        cellEndPos = rowDelimiterPos;
                    } else {
                        cellEndPos = buf.length;
                    }

                    endOfRow = true;
                } else {
                    cellEndPos = nextCellDelimiterPos;
                    endOfRow = false;
                }
            } else {
                if (rowDelimiterPos < 0) {
                    cellEndPos = buf.indexOf(COL_DELIMITER_UINT8, cellEndPos + 1);

                    if (cellEndPos < 0) {
                        cellEndPos = buf.length;
                    }
                } else {
                    cellEndPos = rowDelimiterPos;
                }

                endOfRow = true;
            }

            if (needCols.indexOf(col) >= 0) {
                const cell = buf.subarray(cellBeginPos, cellEndPos);

                result.fill(cell, resultOffset, resultOffset + cell.length);
                resultOffset += cell.length;

                if (col === maxColIdx) {
                    if (!isFinish) {
                        result.fill(ROW_DELIMITER, resultOffset, ++resultOffset);
                    }

                    // JUMP TO NEXT ROW
                    endOfRow = true;
                    cellEndPos = rowDelimiterPos;
                    if (isFinish && nextRowDelimiterPos >= 0) { // if we have a blank line on the end of file
                        result.fill(ROW_DELIMITER, resultOffset, ++resultOffset);
                    }
                    rowDelimiterPos = nextRowDelimiterPos;
                } else {
                    result.fill(COL_DELIMITER, resultOffset, ++resultOffset);
                }
            }

            if (endOfRow) {
                col = 0;
                endOfRow = false;
                rowDelimiterPos = nextRowDelimiterPos;
                nextRowDelimiterPos = buf.indexOf(ROW_DELIMITER_UINT8, rowDelimiterPos + 1);
            } else {
                col++;
            }

            if (cellEndPos < 0 || cellEndPos === buf.length) {
                hasData = false;
            }
        }

        this.prevBuffer = buf.subarray(rowDelimiterPosForPrevBuffer + 1);

        return result.subarray(0, resultOffset);
    }
}
