document.addEventListener('DOMContentLoaded', () => {
    // DOM elements
    const connectionStatus = document.getElementById('connection-status');
    const recordCount = document.getElementById('record-count');
    const paymentData = document.getElementById('payment-data');
    const courierIdInput = document.getElementById('courier-id');
    const lookupCourierButton = document.getElementById('lookup-courier');

    // Track the number of records received
    let recordsReceived = 0;

    // Function to format currency
    const formatCurrency = (amount) => {
        return new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency: 'EUR'
        }).format(amount);
    };

    // Function to add a new payment record to the table
    const addPaymentRecord = (payment) => {
        const row = document.createElement('tr');
        row.className = 'new-record';

        const timestamp = new Date().toLocaleTimeString();

        row.innerHTML = `
            <td>${payment.orderId}</td>
            <td>${formatCurrency(payment.totalAmount)}</td>
            <td>${payment.paymentMethod}</td>
            <td>${timestamp}</td>
        `;

        if (paymentData.firstChild) {
            paymentData.insertBefore(row, paymentData.firstChild);
        } else {
            paymentData.appendChild(row);
        }

        const maxRows = 100;
        while (paymentData.children.length > maxRows) {
            paymentData.removeChild(paymentData.lastChild);
        }

        recordsReceived++;
        recordCount.textContent = `Records: ${recordsReceived}`;
    };

    // Function to connect to the SSE endpoint
    const connectToSSE = () => {
        connectionStatus.textContent = 'Connecting...';
        connectionStatus.className = '';

        // Create a new EventSource connection to the SSE endpoint
        const eventSource = new EventSource('http://localhost:8080/api/payments');

        eventSource.onopen = () => {
            connectionStatus.textContent = 'Connected';
            connectionStatus.className = 'connected';
        };

        eventSource.onmessage = (event) => {
            try {
                const payment = JSON.parse(event.data);
                addPaymentRecord(payment);
            } catch (error) {
                console.error('Error processing payment data:', error);
            }
        };

        eventSource.onerror = () => {
            connectionStatus.textContent = 'Disconnected - Reconnecting...';
            connectionStatus.className = 'disconnected';

            eventSource.close();

            setTimeout(connectToSSE, 5000);
        };
    };

    connectToSSE();

    // Function to handle courier lookup
    const handleCourierLookup = () => {
        const courierId = courierIdInput.value.trim();
        if (courierId) {
            window.location.href = `/courier/${courierId}`;
        } else {
            alert('Please enter a Courier ID');
        }
    };

    // Add event listener for courier lookup button
    lookupCourierButton.addEventListener('click', handleCourierLookup);

    // Add event listener for Enter key in the input field
    courierIdInput.addEventListener('keypress', (event) => {
        if (event.key === 'Enter') {
            handleCourierLookup();
        }
    });
});
