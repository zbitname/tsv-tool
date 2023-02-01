import { Transform, TransformCallback, TransformOptions } from 'stream';

const COL_DELIMITER = Buffer.from('\t');
const ROW_DELIMITER = Buffer.from('\n');
const COL_DELIMITER_UINT8 = COL_DELIMITER.readUInt8(0);
const ROW_DELIMITER_UINT8 = ROW_DELIMITER.readUInt8(0);

export class ColumnSelectorTransform extends Transform {
    private prevBuffer = Buffer.alloc(0);
    private maxColIdx: number;
    private needColIdxs: number[];

    constructor(
        opts: TransformOptions,
        private params: {
            colIndexes: number[];
            cacheBufferMultiplier?: number;
        },
    ) {
        super(opts);
        this.needColIdxs = this.params.colIndexes.sort();
        this.maxColIdx = this.needColIdxs[this.needColIdxs.length - 1];
    }

    // eslint-disable-next-line no-undef
    _transform(chunk: any, encoding: BufferEncoding, callback: TransformCallback): void {
        callback(null, this.__parse(chunk));
    }

    _flush(callback: TransformCallback): void {
        if (this.prevBuffer.length > 0) {
            callback(null, this.__parse(this.prevBuffer, true));
        } else {
            callback(null);
        }
    }

    __parse(chunk: any, isFinish = false) {
        const buf = isFinish ? chunk : Buffer.concat([this.prevBuffer, chunk]);
        const result = Buffer.alloc(buf.byteLength * Math.round(this.params.cacheBufferMultiplier || 1) || 1);
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
