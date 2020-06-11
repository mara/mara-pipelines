// KolorWheel.js - ern0 at linkbroker.hu
// docs: http://linkbroker.hu/stuff/kolorwheel.js/
//
// started 2014.03.04
// updated 2014.03.24


function KolorWheel(color) {

    this.resultList = [this];
    this.elm = null;

    if (typeof (color) == "undefined") color = "#000000";
    if (typeof (color.validateHsl) == "function") {
        this.setHsl([color.h, color.s, color.l]);
    } else {
        this.setColor(color);
    } // else clone

} // KolorWheel() ctor


KolorWheel.prototype.setColor = function (color) {

    if (typeof (color) == "undefined") return;

    if (typeof (color) == "object") {
        this.setHsl(color);
    } else {
        this.setHex(color);
    }

}; // setColor()


KolorWheel.prototype.setHsl = function (hsl) {

    this.h = hsl[0];
    this.s = hsl[1];
    this.l = hsl[2];
    this.validateHsl();

    return this;
}; // setHsl()


KolorWheel.prototype.validateHsl = function () {

    this.h = this.h % 360;
    if (this.h < 0) this.h += 360;

    if (this.s < 0) this.s = 0;
    if (this.s > 100) this.s = 100;
    if (this.l < 0) this.l = 0;
    if (this.l > 100) this.l = 100;

}; // validateHsl()


KolorWheel.prototype.setHex = function (hex) {

    if (hex.substring(0, 1) == "#") hex = hex.substring(1);

    var r = parseInt(hex.substring(0, 2), 16);
    var g = parseInt(hex.substring(2, 4), 16);
    var b = parseInt(hex.substring(4, 6), 16);
    this.setRgb([r, g, b]);

    return this;
}; // setHex()


KolorWheel.prototype.setRgb = function (rgb) {

    var r = rgb[0] / 255;
    var g = rgb[1] / 255;
    var b = rgb[2] / 255;

    var max = Math.max(r, g, b);
    var min = Math.min(r, g, b);
    this.h = (max + min) / 2;
    this.s = this.h;
    this.l = this.h;

    if (max == min) {  // achromatic

        this.h = 0;
        this.s = 0;

    } else {

        var d = max - min;
        this.s = this.l > 0.5 ? d / (2 - max - min) : d / (max + min);

        switch (max) {
            case r:
                this.h = (g - b) / d + (g < b ? 6 : 0);
                break;
            case g:
                this.h = (b - r) / d + 2;
                break;
            case b:
                this.h = (r - g) / d + 4;
                break;
        } // switch

        this.h = this.h / 6;

    } // else achromatic

    this.h = 360 * this.h;
    this.s = 100 * this.s;
    this.l = 100 * this.l;

    return this;
}; // setRgb()


KolorWheel.prototype.hue2rgb = function (p, q, t) {

    if (t < 0) t += 1;
    if (t > 1) t -= 1;
    if (t < 1 / 6) return p + (q - p) * 6 * t;
    if (t < 1 / 2) return q;
    if (t < 2 / 3) return p + (q - p) * (2 / 3 - t) * 6;

    return p;
}; // hue2rgb()


KolorWheel.prototype.getRgb = function () {

    this.validateHsl();

    var h = this.h / 360;
    var s = this.s / 100;
    var l = this.l / 100;

    var r = l;
    var g = l;
    var b = l;

    if (s != 0) {
        var q = l < 0.5 ? l * (1 + s) : l + s - l * s;
        var p = 2 * l - q;
        r = this.hue2rgb(p, q, h + 1 / 3);
        g = this.hue2rgb(p, q, h);
        b = this.hue2rgb(p, q, h - 1 / 3);
    } // if not achromatic

    return [Math.round(r * 255), Math.round(g * 255), Math.round(b * 255)];
}; // getRgb()


KolorWheel.prototype.getHex = function () {

    var result = this.getRgb();

    var hex = this.toHexByte(result[0]);
    hex += this.toHexByte(result[1]);
    hex += this.toHexByte(result[2]);

    return "#" + hex.toUpperCase();
}; // getHex()


KolorWheel.prototype.toHexByte = function (number) {

    var hexByte = number.toString(16);
    if (hexByte.length < 2) hexByte = "0" + hexByte;

    return hexByte;
}; // toHexByte()


KolorWheel.prototype.getHsl = function () {
    this.validateHsl();
    return [this.h, this.s, this.l];
}; // getHsl()


KolorWheel.prototype.multi = function (fn, p1, p2, p3, p4, p5, p6, p7, p8, p9) {

    var sourceList = [].concat(this.resultList);
    this.resultList = [];
    for (var i in sourceList) {
        var src = sourceList[i];
        src.workList = [];
        if (fn == "rel") KolorWheel.prototype.spinSingle.call(src, "rel", p1, p2, p3, p4, p5, p6, p7, p8, p9);
        if (fn == "abs") KolorWheel.prototype.spinSingle.call(src, "abs", p1, p2, p3, p4, p5, p6, p7, p8, p9);
        this.resultList = this.resultList.concat(src.workList);
    }	// foreach sourceList

    if (this.resultList.length == 0) return this;

    var lastResult = this.resultList[this.resultList.length - 1];
    this.h = lastResult.h;
    this.s = lastResult.s;
    this.l = lastResult.l;

    return this;
}; // multi()


KolorWheel.prototype.rel = function (dh, ds, dl, length, start) {
    return this.multi("rel", dh, ds, dl, length, start);
}; // rel()


KolorWheel.prototype.abs = function (dh, ds, dl, length, start) {

    var isDhAColor = false;
    if (typeof (dh) == "object") {
        if (typeof (dh.validateHsl) == "function") isDhAColor = true;
    } else {
        if (("" + dh).substring(0, 1) == "#") isDhAColor = true;
        if (("" + dh).length > 4) isDhAColor = true;
    } // if dh is object

    if (isDhAColor) {
        var conv = new KolorWheel(dh);
        return this.multi("abs", conv.h, conv.s, conv.l, ds, dl);
    } else {
        return this.multi("abs", dh, ds, dl, length, start);
    }

}; // abs()


KolorWheel.prototype.spinSingle = function (mode, dh, ds, dl, length, start) {

    var unchanged = (mode == "abs" ? -1 : 0);
    if (typeof (dh) == "undefined") dh = unchanged;
    if (typeof (ds) == "undefined") ds = unchanged;
    if (typeof (dl) == "undefined") dl = unchanged;

    if (typeof (dh) == "undefined") length = 12;
    var dhLength = 0;
    var dsLength = 0;
    var dlLength = 0;
    if (typeof (dh) == "object") dhLength = dh.length;
    if (typeof (ds) == "object") dsLength = ds.length;
    if (typeof (dl) == "object") dlLength = dl.length;

    if (typeof (length) == "undefined") {
        length = 1;
        if (dhLength > length) length = dhLength;
        if (dsLength > length) length = dsLength;
        if (dlLength > length) length = dlLength;
    }
    if (typeof (start) == "undefined") start = 0;

    var jquery = null;
    if (typeof (length) == "object") {
        jquery = length;
        length = jquery.length;
    }

    for (step = start; step < length; step++) {

        var result = new KolorWheel(this);

        var progress = (length == 1 ? 1 : step / (length - 1));
        var parmh;
        var parms;
        var parml;

        if (dhLength > 0) {
            parmh = dh[step % dhLength];
        } else {
            parmh = dh * progress;
        }

        if (dsLength > 0) {
            parms = ds[step % dsLength];
        } else {
            parms = ds * progress;
        }

        if (dlLength > 0) {
            parml = dl[step % dlLength];
        } else {
            parml = dl * progress;
        }

        if (mode == "rel") {
            result.h += parmh;
            result.s += parms;
            result.l += parml;
        } // if rel
        else {
            if (dh == unchanged) {
                result.h = this.h;
            } else {
                if (dhLength == 0) {
                    result.h = this.calcLinearGradientStep(step, length, this.h, dh);
                } else {
                    result.h = parmh;
                }
            }
            if (ds == unchanged) {
                result.s = this.s;
            } else {
                if (dsLength == 0) {
                    result.s = this.calcLinearGradientStep(step, length, this.s, ds);
                } else {
                    result.s = parms;
                }
            }
            if (dl == unchanged) {
                result.l = this.l;
            } else {
                if (dlLength == 0) {
                    result.l = this.calcLinearGradientStep(step, length, this.l, dl);
                } else {
                    result.l = parml;
                }
            }
        } // else abs

        result.step = step;
        if (jquery) result.elm = jquery.eq(step);

        this.workList[step] = result;
    } // for step

}; // spinSingle()


KolorWheel.prototype.calcLinearGradientStep = function (step, length, base, target) {

    var progress = (step / (length - 1));
    var result = base + ((target - base) * progress);

    return result;
}; // calcLinearGradientStep();


KolorWheel.prototype.each = function (fn) {

    for (var i in this.resultList) {
        fn.call(this.resultList[i], this.resultList[i].elm);
    } // foreach result

}; // each()


KolorWheel.prototype.get = function (n) {
    if (typeof (n) == "undefined") n = 0;
    return this.resultList[n];
}; // get()


KolorWheel.prototype.isDark = function () {
    return (!this.isLight());
}; // isDark()


KolorWheel.prototype.isLight = function () {

    var rgb = this.getRgb();
    var lum = (0.299 * rgb[0]) + (0.587 * rgb[1]) + (0.114 * rgb[2]);

    return (lum > 127);
}; // isLight()