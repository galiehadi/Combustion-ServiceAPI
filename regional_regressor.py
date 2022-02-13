from numpy import array, argsort

class RegionalLinearReg(object):
    def __init__(self, X, Y):
        self.length = min(len(X), len(Y))
        self.X = array(X[:self.length])
        self.X = self.X[argsort(self.X)]
        self.Y = array(Y[:self.length])
        self.Y = self.Y[argsort(self.X)]
        self.rangelo = min(self.X)
        self.rangehi = max(self.X)
    
    def _map_(self, value, in_min, in_max, out_min, out_max):
        return (value - in_min) * (out_max - out_min) / (in_max - in_min) + out_min
        
    def predict(self, x):
        if x <= self.rangelo:
            return self.Y[0]
        elif x >= self.rangehi:
            return self.Y[-1]
        for i in range(1,len(self.X)):
            if x < self.X[i] and x > self.X[i-1]:
                return self._map_(x, self.X[i-1], self.X[i], self.Y[i-1], self.Y[i])
        return f'Error using value: {x}'