import { useState } from 'react'

const API_URL = import.meta.env.DEV ? '/api' : 'http://localhost:8080'

function App() {
  const [scheme, setScheme] = useState('TPC')
  const [amountType, setAmountType] = useState('amountRequired')
  const [amount, setAmount] = useState('140')
  const [tenorMonths, setTenorMonths] = useState('6')
  const [disbursementDate, setDisbursementDate] = useState(() => {
    const d = new Date()
    return d.toISOString().slice(0, 10)
  })
  const [firstRepaymentDate, setFirstRepaymentDate] = useState(() => {
    const d = new Date()
    d.setMonth(d.getMonth() + 1)
    return d.toISOString().slice(0, 10)
  })
  const [result, setResult] = useState(null)
  const [error, setError] = useState(null)
  const [loading, setLoading] = useState(false)

  const calculate = async () => {
    setError(null)
    setResult(null)
    setLoading(true)
    try {
      const response = await fetch(`${API_URL}/calculate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          scheme,
          amountType: amountType === 'principal' ? 'principal' : 'amountRequired',
          amount: parseFloat(amount) || 0,
          tenorMonths: parseInt(tenorMonths, 10) || 6,
          disbursementDate,
          firstRepaymentDate,
        }),
      })
      const text = await response.text()
      let data
      try {
        data = text ? JSON.parse(text) : {}
      } catch {
        throw new Error(
          response.ok
            ? 'Invalid response from server'
            : `Server error (${response.status}). Is the Java engine running? Run: java LoanServer`
        )
      }
      if (!response.ok) {
        const msg = data?.error || `Calculation failed (${response.status})`
        throw new Error(msg)
      }
      setResult(data)
    } catch (err) {
      const msg = err.message || ''
      const isConnectionError = /fetch|ECONNREFUSED|network|connection/i.test(msg)
      setError(
        isConnectionError
          ? 'Cannot connect to the loan engine. Start it with: java LoanServer (in the LMS-Project folder)'
          : msg
      )
    } finally {
      setLoading(false)
    }
  }

  return (
    <div style={{ padding: '24px', fontFamily: 'system-ui, sans-serif', maxWidth: 900, margin: '0 auto' }}>
      <h1 style={{ color: '#1e3a8a', marginBottom: 4 }}>Loan Management System</h1>
      <p style={{ color: '#64748b', marginBottom: 24 }}>Standardised Term Loan</p>

      <div style={{
        background: '#f8fafc',
        padding: 20,
        borderRadius: 8,
        marginBottom: 24,
        border: '1px solid #e2e8f0',
      }}>
        <h3 style={{ marginTop: 0, marginBottom: 16 }}>Loan parameters</h3>
        <div style={{ display: 'grid', gap: 16, gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))' }}>
          <div>
            <label style={{ display: 'block', marginBottom: 4, fontSize: 14, fontWeight: 500 }}>Scheme</label>
            <select
              value={scheme}
              onChange={(e) => setScheme(e.target.value)}
              style={{ width: '100%', padding: 8, borderRadius: 4, border: '1px solid #cbd5e1' }}
            >
              <option value="TPC">TPC (7% interest, 5% admin)</option>
              <option value="SSB">SSB (7% interest, 7% admin)</option>
            </select>
          </div>
          <div>
            <label style={{ display: 'block', marginBottom: 4, fontSize: 14, fontWeight: 500 }}>Amount type</label>
            <select
              value={amountType}
              onChange={(e) => setAmountType(e.target.value)}
              style={{ width: '100%', padding: 8, borderRadius: 4, border: '1px solid #cbd5e1' }}
            >
              <option value="amountRequired">Amount required (net)</option>
              <option value="principal">Principal amount</option>
            </select>
          </div>
          <div>
            <label style={{ display: 'block', marginBottom: 4, fontSize: 14, fontWeight: 500 }}>
              {amountType === 'principal' ? 'Principal amount' : 'Amount required'}
            </label>
            <input
              type="number"
              value={amount}
              onChange={(e) => setAmount(e.target.value)}
              min="0"
              step="0.01"
              style={{ width: '100%', padding: 8, borderRadius: 4, border: '1px solid #cbd5e1' }}
            />
          </div>
          {amountType === 'principal' && (
            <div style={{
              padding: '8px 12px',
              background: '#f1f5f9',
              borderRadius: 4,
              border: '1px solid #e2e8f0',
            }}>
              <label style={{ display: 'block', marginBottom: 4, fontSize: 12, color: '#64748b' }}>Amount requested</label>
              <span style={{ fontSize: 16, fontWeight: 600 }}>
                {((parseFloat(amount) || 0) * (1 - (scheme === 'SSB' ? 0.07 : 0.05))).toLocaleString('en-US', { minimumFractionDigits: 2 })}
              </span>
            </div>
          )}
          <div>
            <label style={{ display: 'block', marginBottom: 4, fontSize: 14, fontWeight: 500 }}>Tenor (months)</label>
            <input
              type="number"
              value={tenorMonths}
              onChange={(e) => setTenorMonths(e.target.value)}
              min="1"
              max="120"
              style={{ width: '100%', padding: 8, borderRadius: 4, border: '1px solid #cbd5e1' }}
            />
          </div>
          <div>
            <label style={{ display: 'block', marginBottom: 4, fontSize: 14, fontWeight: 500 }}>Disbursement date</label>
            <input
              type="date"
              value={disbursementDate}
              onChange={(e) => setDisbursementDate(e.target.value)}
              style={{ width: '100%', padding: 8, borderRadius: 4, border: '1px solid #cbd5e1' }}
            />
          </div>
          <div>
            <label style={{ display: 'block', marginBottom: 4, fontSize: 14, fontWeight: 500 }}>Date of first repayment</label>
            <input
              type="date"
              value={firstRepaymentDate}
              onChange={(e) => setFirstRepaymentDate(e.target.value)}
              style={{ width: '100%', padding: 8, borderRadius: 4, border: '1px solid #cbd5e1' }}
            />
          </div>
        </div>
        <button
          onClick={calculate}
          disabled={loading}
          style={{
            marginTop: 16,
            padding: '10px 24px',
            fontSize: 16,
            backgroundColor: '#16a34a',
            color: 'white',
            border: 'none',
            borderRadius: 6,
            cursor: loading ? 'not-allowed' : 'pointer',
            fontWeight: 600,
          }}
        >
          {loading ? 'Calculating…' : 'Calculate'}
        </button>
      </div>

      {error && (
        <div style={{
          padding: 16,
          background: '#fef2f2',
          color: '#dc2626',
          borderRadius: 8,
          marginBottom: 24,
          border: '1px solid #fecaca',
        }}>
          {error}
        </div>
      )}

      {result && (
        <>
          <div style={{
            background: '#ecfdf5',
            padding: 20,
            borderRadius: 8,
            marginBottom: 24,
            border: '1px solid #a7f3d0',
          }}>
            <h3 style={{ marginTop: 0, marginBottom: 16 }}>Loan summary</h3>
            <div style={{ display: 'grid', gap: 8, gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))' }}>
              <div><strong>Amount required:</strong> {result.amountRequired.toLocaleString('en-US', { minimumFractionDigits: 2 })}</div>
              <div><strong>Facility amount:</strong> {result.facilityAmount.toLocaleString('en-US', { minimumFractionDigits: 2 })}</div>
              <div><strong>Effective interest rate:</strong> {result.effectiveInterestPercent}% per month</div>
              <div><strong>Administration fees:</strong> {result.adminPercent}%</div>
              <div><strong>Scheme:</strong> {result.scheme}</div>
              <div><strong>Tenor:</strong> {result.tenorMonths} months</div>
              <div><strong>Disbursement date:</strong> {result.disbursementDate}</div>
              <div><strong>Date of first repayment:</strong> {result.firstRepaymentDate}</div>
              <div><strong>End date:</strong> {result.endDate}</div>
              <div style={{ color: '#166534', fontWeight: 700 }}>
                <strong>Monthly instalment:</strong> {result.monthlyInstallment.toLocaleString('en-US', { minimumFractionDigits: 2 })}
              </div>
            </div>
          </div>

          <div style={{ overflowX: 'auto' }}>
            <h3 style={{ marginBottom: 12 }}>Amortisation schedule</h3>
            <table style={{
              width: '100%',
              borderCollapse: 'collapse',
              fontSize: 14,
              background: 'white',
              borderRadius: 8,
              overflow: 'hidden',
              boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
            }}>
              <thead>
                <tr style={{ background: '#1e3a8a', color: 'white' }}>
                  <th style={{ padding: 10, textAlign: 'left' }}>Period</th>
                  <th style={{ padding: 10, textAlign: 'left' }}>Due Date</th>
                  <th style={{ padding: 10, textAlign: 'right' }}>Payment</th>
                  <th style={{ padding: 10, textAlign: 'right' }}>Interest</th>
                  <th style={{ padding: 10, textAlign: 'right' }}>Principal</th>
                  <th style={{ padding: 10, textAlign: 'right' }}>Principal Balance</th>
                  <th style={{ padding: 10, textAlign: 'right' }}>Outstanding Balance</th>
                </tr>
              </thead>
              <tbody>
                {result.schedule?.map((row, i) => (
                  <tr key={`${row.period}-${i}`} style={{ borderBottom: '1px solid #e2e8f0', background: i % 2 === 1 ? '#f8fafc' : 'white' }}>
                    <td style={{ padding: 10 }}>{row.period}</td>
                    <td style={{ padding: 10 }}>{row.dueDate}</td>
                    <td style={{ padding: 10, textAlign: 'right' }}>{(row.payment ?? 0).toLocaleString('en-US', { minimumFractionDigits: 2 })}</td>
                    <td style={{ padding: 10, textAlign: 'right' }}>{(row.interest ?? 0).toLocaleString('en-US', { minimumFractionDigits: 2 })}</td>
                    <td style={{ padding: 10, textAlign: 'right' }}>{(row.principal ?? 0).toLocaleString('en-US', { minimumFractionDigits: 2 })}</td>
                    <td style={{ padding: 10, textAlign: 'right' }}>{(row.principalBalance ?? 0).toLocaleString('en-US', { minimumFractionDigits: 2 })}</td>
                    <td style={{ padding: 10, textAlign: 'right' }}>{(row.outstandingBalance ?? 0).toLocaleString('en-US', { minimumFractionDigits: 2 })}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  )
}

export default App
