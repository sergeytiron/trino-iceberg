// This file provides a replacement for Microsoft.Extensions.Logging.Internal.FormattedLogValues
// which was made internal in newer versions of Microsoft.Extensions.Logging.Abstractions.
// It is used by the Trino.Client library source files compiled into this project.

using System.Collections;

namespace Microsoft.Extensions.Logging.Internal;

/// <summary>
/// A simple implementation of formatted log values that replaces the internal
/// Microsoft.Extensions.Logging.Internal.FormattedLogValues class.
/// </summary>
internal readonly struct FormattedLogValues : IReadOnlyList<KeyValuePair<string, object?>>
{
    private readonly string _format;
    private readonly object?[] _values;

    public FormattedLogValues(string format, params object?[] values)
    {
        _format = format ?? string.Empty;
        _values = values ?? [];
    }

    public int Count => _values.Length + 1;

    public KeyValuePair<string, object?> this[int index]
    {
        get
        {
            if (index < 0 || index >= Count)
                throw new IndexOutOfRangeException(nameof(index));

            if (index == Count - 1)
                return new KeyValuePair<string, object?>("{OriginalFormat}", _format);

            return new KeyValuePair<string, object?>($"{{{index}}}", _values[index]);
        }
    }

    public override string ToString()
    {
        if (_values.Length == 0)
            return _format;

        try
        {
            return string.Format(_format, _values);
        }
        catch (FormatException)
        {
            return _format;
        }
    }

    public IEnumerator<KeyValuePair<string, object?>> GetEnumerator()
    {
        for (int i = 0; i < Count; i++)
        {
            yield return this[i];
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
