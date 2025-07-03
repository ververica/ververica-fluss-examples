document.addEventListener('DOMContentLoaded', () => {
    // DOM elements
    const connectionStatus = document.getElementById('connection-status');
    const recordCount = document.getElementById('record-count');
    const paymentData = document.getElementById('payment-data');

    // Track the number of records received
    let recordsReceived = 0;

    // Function to format currency
    const formatCurrency = (amount) => {
        return new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency: 'USD'
        }).format(amount);
    };

    // Function to add a new payment record to the table
    const addPaymentRecord = (payment) => {
        // Create a new row
        const row = document.createElement('tr');
        row.className = 'new-record';

        // Create timestamp
        const timestamp = new Date().toLocaleTimeString();

        // Set the row content
        row.innerHTML = `
            <td>${payment.orderId}</td>
            <td>${formatCurrency(payment.totalAmount)}</td>
            <td>${payment.paymentMethod}</td>
            <td>${timestamp}</td>
        `;

        // Add the row to the table (at the beginning)
        if (paymentData.firstChild) {
            paymentData.insertBefore(row, paymentData.firstChild);
        } else {
            paymentData.appendChild(row);
        }

        // Limit the number of visible rows to prevent performance issues
        const maxRows = 100;
        while (paymentData.children.length > maxRows) {
            paymentData.removeChild(paymentData.lastChild);
        }

        // Update the record count
        recordsReceived++;
        recordCount.textContent = `Records: ${recordsReceived}`;
    };

    // Function to connect to the SSE endpoint
    const connectToSSE = () => {
        connectionStatus.textContent = 'Connecting...';
        connectionStatus.className = '';

        // Create a new EventSource connection to the SSE endpoint
        const eventSource = new EventSource('http://localhost:8080/api/payments');

        // Handle connection open
        eventSource.onopen = () => {
            connectionStatus.textContent = 'Connected';
            connectionStatus.className = 'connected';
        };

        // Handle incoming messages
        eventSource.onmessage = (event) => {
            try {
                const payment = JSON.parse(event.data);
                addPaymentRecord(payment);
            } catch (error) {
                console.error('Error processing payment data:', error);
            }
        };

        // Handle errors
        eventSource.onerror = () => {
            connectionStatus.textContent = 'Disconnected - Reconnecting...';
            connectionStatus.className = 'disconnected';

            // Close the current connection
            eventSource.close();

            // Try to reconnect after a delay
            setTimeout(connectToSSE, 5000);
        };
    };

    // Start the SSE connection
    connectToSSE();
});
