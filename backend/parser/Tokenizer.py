'''
分词器
'''
class Tokenizer(object):
    def __init__(self, stat: bytearray | bytes):
        self.stat = stat
        self.pos = 0
        self.currentToken = ""
        self.flushToken = True
        self.err = None

    def peek(self) -> str:
        if self.err is not None:
            raise self.err
        if self.flushToken:
            try:
                token = self.next()
            except Exception as e:
                self.err = e
                raise e
            self.currentToken = token
            self.flushToken = False
        return self.currentToken

    def pop(self) -> None:
        self.flushToken = True

    def errStat(self) -> bytearray:
        res = bytearray(len(self.stat) + 3)
        res[0 : self.pos] = self.stat[0 : self.pos]
        res[self.pos : self.pos + 3] = b"<< "
        res[self.pos + 3 : ] = self.stat[self.pos : ]
        return res
    
    def popByte(self) -> None:
        self.pos += 1
        if self.pos > len(self.stat):
            self.pos = len(self.stat)
    
    def peekByte(self) -> None | int:
        if self.pos == len(self.stat):
            return None
        return self.stat[self.pos]
    
    def next(self) -> str:
        if self.err is not None:
            raise self.err
        return self.nextMetaState()
    
    def nextMetaState(self) -> str:
        while True:
            b = self.peekByte()
            if b is None:
                return ""
            # 直到找到一个非空格/换行符
            if not self.isBlank(b):
                break
            self.popByte()
        b = self.peekByte()
        if self.isSymbol(b):
            self.popByte()
            return chr(b)
        elif b == ord('"') or b == ord("'"):
            return self.nextQuoteState()
        elif self.isAlphaBeta(b) or self.isDigit(b):
            return self.nextTokenState()
        else:
            self.err = Exception("InvalidCommandException")
            raise self.err
    
    def nextTokenState(self) -> str:
        sb = []
        while True:
            b = self.peekByte()
            if b is None or not (self.isAlphaBeta(b) or self.isDigit(b) or b == ord('_')):
                if b is not None and self.isBlank(b):
                    self.popByte()
                return ''.join(sb)
            sb.append(chr(b))
            self.popByte()
    
    def isDigit(self, b: int) -> bool:
        return (b >= ord('0') and b <= ord('9'))

    def isAlphaBeta(self, b: int) -> bool:
        return ((b >= ord('a') and b <= ord('z')) or (b >= ord('A') and b <= ord('Z')))
    
    def isSymbol(self, b: int) -> bool:
        return (b == ord('>') or b == ord('<') or b == ord('=') or
                b == ord('*') or b == ord(',') or b == ord('(') or b == ord(')'))
    
    def isBlank(self, b: int) -> bool:
        return (b == ord('\n') or b == ord(' ') or b == ord('\t'))
    
    def nextQuoteState(self) -> str:
        quote = self.peekByte()
        if quote is None:
            self.err = Exception("Invalid command")
            raise self.err
        self.popByte()
        sb = []
        while True:
            b = self.peekByte()
            if b is None:
                self.err = Exception("InvalidCommandException")
                raise self.err
            if b == quote:
                self.popByte()
                return ''.join(sb)
            sb.append(chr(b))
            self.popByte()