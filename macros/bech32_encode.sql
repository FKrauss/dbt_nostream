
{# Macro to create a BigQuery UDF for bech32 encoding #}

{% macro create_bech32_udf() %}
  {{ return(adapter.dispatch('create_bech32_udf')()) }}
{% endmacro %}

{% macro default__create_bech32_udf() %}
  {# Default implementation - basic placeholder #}
  select 'bech32_encode UDF not implemented for this adapter' as error
{% endmacro %}

{% macro bigquery__create_bech32_udf() %}
  create or replace function {{ target.dataset }}.bech32_encode(input_string string)
  returns string
  language js as r"""
    // Bech32 encoding implementation
    const CHARSET = 'qpzry9x8gf2tvdw0s3jn54khce6mua7l';
    
    function bech32Polymod(values) {
      const GEN = [0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3];
      let chk = 1;
      for (let value of values) {
        let top = chk >> 25;
        chk = (chk & 0x1ffffff) << 5 ^ value;
        for (let i = 0; i < 5; i++) {
          chk ^= ((top >> i) & 1) ? GEN[i] : 0;
        }
      }
      return chk;
    }
    
    function bech32HrpExpand(hrp) {
      let ret = [];
      for (let p = 0; p < hrp.length; p++) {
        ret.push(hrp.charCodeAt(p) >> 5);
      }
      ret.push(0);
      for (let p = 0; p < hrp.length; p++) {
        ret.push(hrp.charCodeAt(p) & 31);
      }
      return ret;
    }
    
    function bech32VerifyChecksum(hrp, data) {
      return bech32Polymod(bech32HrpExpand(hrp).concat(data)) === 1;
    }
    
    function bech32CreateChecksum(hrp, data) {
      let values = bech32HrpExpand(hrp).concat(data).concat([0, 0, 0, 0, 0, 0]);
      let mod = bech32Polymod(values) ^ 1;
      let ret = [];
      for (let p = 0; p < 6; p++) {
        ret.push((mod >> 5 * (5 - p)) & 31);
      }
      return ret;
    }
    
    function bech32Encode(hrp, data) {
      let combined = data.concat(bech32CreateChecksum(hrp, data));
      let ret = hrp + '1';
      for (let p = 0; p < combined.length; p++) {
        ret += CHARSET.charAt(combined[p]);
      }
      return ret;
    }
    
    function convertBits(data, frombits, tobits, pad) {
      let acc = 0;
      let bits = 0;
      let ret = [];
      let maxv = (1 << tobits) - 1;
      let max_acc = (1 << (frombits + tobits - 1)) - 1;
      for (let value of data) {
        if (value < 0 || (value >> frombits)) {
          return null;
        }
        acc = ((acc << frombits) | value) & max_acc;
        bits += frombits;
        while (bits >= tobits) {
          bits -= tobits;
          ret.push((acc >> bits) & maxv);
        }
      }
      if (pad) {
        if (bits) {
          ret.push((acc << (tobits - bits)) & maxv);
        }
      } else if (bits >= frombits || ((acc << (tobits - bits)) & maxv)) {
        return null;
      }
      return ret;
    }
    
    // Convert input string to bytes
    let bytes = [];
    for (let i = 0; i < input_string.length; i++) {
      bytes.push(input_string.charCodeAt(i));
    }
    
    // Convert to 5-bit groups
    let converted = convertBits(bytes, 8, 5, true);
    if (!converted) {
      return null;
    }
    
    // Encode with 'bc' as human readable part (common for Bitcoin addresses)
    return bech32Encode('bc', converted);
  """;
{% endmacro %}

{# Helper macro to use the bech32 encoding function #}
{% macro bech32_encode(column_name, hrp='bc') %}
  {{ target.dataset }}.bech32_encode({{ column_name }})
{% endmacro %}
