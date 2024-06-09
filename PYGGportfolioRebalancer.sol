// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/utils/math/SafeMath.sol";
import "@openzeppelin/contracts/security/Pausable.sol";
import "@openzeppelin/contracts/access/AccessControl.sol";
import "@chainlink/contracts/src/v0.8/interfaces/AggregatorV3Interface.sol";
import "@uniswap/v2-periphery/contracts/interfaces/IUniswapV2Router02.sol";
import "@uniswap/v3-periphery/contracts/interfaces/ISwapRouter.sol";
import "@uniswap/v3-core/contracts/interfaces/IUniswapV3Pool.sol";
import "@uniswap/v3-core/contracts/interfaces/IUniswapV3Factory.sol";

contract PYGGportfolioRebalancer is Ownable, ReentrancyGuard, Pausable, AccessControl {
    using SafeMath for uint256;
    using SafeERC20 for IERC20;

    struct Trade {
        address user;
        address tokenIn;
        address tokenOut;
        uint256 amountIn;
        string tradeType; // "deposit" or "rebalance" or "liquidation"
        uint256 retryCounter;
        uint256 nextRetryTime;
        bool isHighGas; // Flag to indicate if retry is due to high gas
        bool isSlippage; // Flag to indicate if retry is due to slippage
    }

    struct UserBalance {
        uint256 eth;
    }

    address[] public tokens;
    IUniswapV2Router02 public uniswapV2Router;
    ISwapRouter public uniswapV3Router;
    IUniswapV3Factory public uniswapV3Factory;
    AggregatorV3Interface internal gasPriceFeed;

    uint256 public depositFee;
    uint256 public withdrawalFee;
    uint256 public minimumGasETH;
    uint256 public minimumLiquidationGasETH;
    uint256 public slippageTolerance; // Slippage tolerance in basis points (e.g., 50 means 0.5%)
    uint256 public gasPriceThreshold; // Gas price threshold for queuing trades
    uint256 public maxRetryAttempts = 120; // Maximum retry attempts for slippage

    uint256 public accumulatedETHFees;

    address private constant UNISWAP_V2_ROUTER = 0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D;
    address private constant UNISWAP_V3_ROUTER = 0xE592427A0AEce92De3Edee1F18E0157C05861564;
    address private constant UNISWAP_V3_FACTORY = 0x1F98431c8aD98523631AE4a59f267346ea31F984;

    Trade[] public tradeQueue;

    mapping(address => UserBalance) private userBalances;

    bytes32 public constant MANAGER_ROLE = keccak256("MANAGER_ROLE");

    event Deposited(address indexed user, uint256 amount, string tokenType);
    event Rebalanced(address indexed user, uint256 timestamp);
    event TokenSwapped(address indexed token, uint256 amount, uint256 receivedAmount);
    event Withdrawn(address indexed user, uint256 amount, string tokenType);
    event FeesWithdrawn(address indexed owner, uint256 amount, string tokenType);
    event MinimumGasETHSet(uint256 amount);
    event MinimumLiquidationGasETHSet(uint256 amount);
    event GasETHWithdrawn(address indexed owner, uint256 amount);
    event Liquidated(uint256 totalEthAmount);
    event GasPriceThresholdSet(uint256 threshold);
    event TradeQueued(address indexed user, address tokenIn, address tokenOut, uint256 amountIn, string tradeType);
    event TradeProcessed(address indexed user, address tokenIn, address tokenOut, uint256 amountIn, string tradeType);
    event TradeFailed(address indexed user, address tokenIn, address tokenOut, uint256 amountIn, string tradeType, string reason);
    event MaxRetryAttemptsSet(uint256 attempts);
    event EmergencyWithdrawn(address indexed owner, uint256 ethAmount, string tokenType);
    event EmergencyTokenWithdrawn(address indexed owner, address indexed token, uint256 amount);

    constructor() {
        _setupRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _setupRole(MANAGER_ROLE, msg.sender);

        uniswapV2Router = IUniswapV2Router02(UNISWAP_V2_ROUTER);
        uniswapV3Router = ISwapRouter(UNISWAP_V3_ROUTER);
        uniswapV3Factory = IUniswapV3Factory(UNISWAP_V3_FACTORY);
        gasPriceFeed = AggregatorV3Interface(0xYourChainlinkGasPriceOracleAddress); // Replace with actual Chainlink Gas Price Oracle address
    }

    modifier onlyWhitelisted() {
        require(isWhitelisted(msg.sender), "Address not whitelisted");
        _;
    }

    function isWhitelisted(address user) public view returns (bool) {
        // Add logic for checking if a user is whitelisted
        return true;
    }

    function setTokens(address[] calldata _tokens) external onlyRole(MANAGER_ROLE) {
        require(_tokens.length >= 10 && _tokens.length <= 150, "Tokens length must be between 10 and 150");
        tokens = _tokens;
    }

    function addToWhitelist(address user) external onlyRole(MANAGER_ROLE) {
        // Add user to whitelist logic
    }

    function removeFromWhitelist(address user) external onlyRole(MANAGER_ROLE) {
        // Remove user from whitelist logic
    }

    function setDepositFee(uint256 fee) external onlyRole(MANAGER_ROLE) {
        require(fee <= 300, "Deposit fee must be less than or equal to 3%");
        depositFee = fee;
    }

    function setWithdrawalFee(uint256 fee) external onlyRole(MANAGER_ROLE) {
        require(fee <= 300, "Withdrawal fee must be less than or equal to 3%");
        withdrawalFee = fee;
    }

    function setMinimumGasETH(uint256 amount) external onlyRole(MANAGER_ROLE) {
        require(amount <= 5 ether, "Minimum GasETH must be less than or equal to 5 ETH");
        minimumGasETH = amount;
        emit MinimumGasETHSet(amount);
    }

    function setMinimumLiquidationGasETH(uint256 amount) external onlyRole(MANAGER_ROLE) {
        require(amount <= 5 ether, "Minimum LiquidationGasETH must be less than or equal to 5 ETH");
        minimumLiquidationGasETH = amount;
        emit MinimumLiquidationGasETHSet(amount);
    }

    function setGasPriceThreshold(uint256 threshold) external onlyRole(MANAGER_ROLE) {
        gasPriceThreshold = threshold;
        emit GasPriceThresholdSet(threshold);
    }

    function setSlippageTolerance(uint256 tolerance) external onlyRole(MANAGER_ROLE) {
        require(tolerance <= 1000, "Slippage tolerance must be less than or equal to 10%"); // 1000 basis points = 10%
        slippageTolerance = tolerance;
    }

    function setMaxRetryAttempts(uint256 attempts) external onlyRole(MANAGER_ROLE) {
        require(attempts > 0, "Max retry attempts must be greater than zero");
        maxRetryAttempts = attempts;
        emit MaxRetryAttemptsSet(attempts);
    }

    function getCurrentGasPrice() public view returns (uint256) {
        (, int256 price,,,) = gasPriceFeed.latestRoundData();
        require(price > 0, "Invalid gas price");
        return uint256(price);
    }

    function depositETH() external payable nonReentrant onlyWhitelisted whenNotPaused {
        require(msg.value > 0, "ETH amount must be greater than zero");

        if (getCurrentGasPrice() > gasPriceThreshold) {
            tradeQueue.push(Trade(msg.sender, address(0), address(0), msg.value, "deposit", 0, block.timestamp + 1 hours, true, false));
            emit TradeQueued(msg.sender, address(0), address(0), msg.value, "deposit");
        } else {
            executeDepositETH(msg.sender, msg.value);
        }
    }

    function executeDepositETH(address user, uint256 amount) internal {
        require(amount > 0, "Amount must be greater than zero");
        uint256 fee = 0;
        if (user != owner()) {
            unchecked {
                fee = amount.mul(depositFee).div(10000);
                accumulatedETHFees = accumulatedETHFees.add(fee);
            }
        }
        uint256 netAmount = amount.sub(fee);
        require(address(this).balance >= minimumGasETH, "Insufficient gas ETH");
        _topUpGasETH(netAmount);
        userBalances[user].eth = userBalances[user].eth.add(netAmount);
        emit Deposited(user, amount, "ETH");
    }

    function _topUpGasETH(uint256 amount) internal {
        if (address(this).balance < minimumGasETH) {
            unchecked {
                uint256 topUpAmount = minimumGasETH.sub(address(this).balance);
                uint256 toDeposit = amount >= topUpAmount ? topUpAmount : amount;
                // Use the remaining amount for other purposes
            }
        }
    }

    function _swapAndRebalance(address tokenAddress, uint256 amount, string memory tokenType) internal {
        uint256 tokenCount = tokens.length;
        require(tokenCount > 0, "No tokens set for the portfolio");

        uint256 equalAmount = amount.div(tokenCount);
        require(equalAmount > 0, "Equal amount is too small");

        for (uint256 i = 0; i < tokens.length; i++) {
            address token = tokens[i];

            if (tokenAddress != address(0)) {
                IERC20(tokenAddress).safeApprove(address(uniswapV2Router), equalAmount);
                IERC20(tokenAddress).safeApprove(address(uniswapV3Router), equalAmount);
            }

            (bool useV2, uint256 amountOut) = getBestSwapOption(equalAmount, token, tokenAddress);
            uint256 minAmountOut = amountOut.mul(uint256(10000).sub(slippageTolerance)).div(10000);

            if (useV2) {
                address[] memory path = new address[](2);
                path[0] = tokenAddress == address(0) ? uniswapV2Router.WETH() : tokenAddress;
                path[1] = token;

                uint256 deadline = block.timestamp + 15;

                if (tokenAddress == address(0)) {
                    try uniswapV2Router.swapExactETHForTokensSupportingFeeOnTransferTokens{value: equalAmount}(
                        minAmountOut,
                        path,
                        address(this),
                        deadline
                    ) {
                        emit TokenSwapped(token, equalAmount, amountOut);
                    } catch {
                        tradeQueue.push(Trade(msg.sender, tokenAddress, token, equalAmount, "rebalance", 0, block.timestamp + 1 hours, false, true));
                        emit TradeQueued(msg.sender, tokenAddress, token, equalAmount, "rebalance");
                    }
                } else {
                    try uniswapV2Router.swapExactTokensForTokensSupportingFeeOnTransferTokens(
                        equalAmount,
                        minAmountOut,
                        path,
                        address(this),
                        deadline
                    ) {
                        emit TokenSwapped(token, equalAmount, amountOut);
                    } catch {
                        tradeQueue.push(Trade(msg.sender, tokenAddress, token, equalAmount, "rebalance", 0, block.timestamp + 1 hours, false, true));
                        emit TradeQueued(msg.sender, tokenAddress, token, equalAmount, "rebalance");
                    }
                }
            } else {
                ISwapRouter.ExactInputSingleParams memory params = ISwapRouter.ExactInputSingleParams({
                    tokenIn: tokenAddress == address(0) ? uniswapV2Router.WETH() : tokenAddress,
                    tokenOut: token,
                    fee: 3000,
                    recipient: address(this),
                    deadline: block.timestamp + 15,
                    amountIn: equalAmount,
                    amountOutMinimum: minAmountOut,
                    sqrtPriceLimitX96: 0
                });

                if (tokenAddress == address(0)) {
                    try uniswapV3Router.exactInputSingle{value: equalAmount}(params) {
                        emit TokenSwapped(token, equalAmount, amountOut);
                    } catch {
                        tradeQueue.push(Trade(msg.sender, tokenAddress, token, equalAmount, "rebalance", 0, block.timestamp + 1 hours, false, true));
                        emit TradeQueued(msg.sender, tokenAddress, token, equalAmount, "rebalance");
                    }
                } else {
                    try uniswapV3Router.exactInputSingle(params) {
                        emit TokenSwapped(token, equalAmount, amountOut);
                    } catch {
                        tradeQueue.push(Trade(msg.sender, tokenAddress, token, equalAmount, "rebalance", 0, block.timestamp + 1 hours, false, true));
                        emit TradeQueued(msg.sender, tokenAddress, token, equalAmount, "rebalance");
                    }
                }
            }
        }

        emit Deposited(msg.sender, amount, tokenType);
    }

    function rebalance() external onlyRole(MANAGER_ROLE) nonReentrant whenNotPaused {
        if (getCurrentGasPrice() > gasPriceThreshold) {
            tradeQueue.push(Trade(msg.sender, address(0), address(0), 0, "rebalance", 0, block.timestamp + 1 hours, true, false));
            emit TradeQueued(msg.sender, address(0), address(0), 0, "rebalance");
        } else {
            executeRebalance();
        }
    }

    function executeRebalance() internal {
        uint256 totalValue = 0;
        uint256[] memory values = new uint256[](tokens.length);

        for (uint256 i = 0; i < tokens.length; i++) {
            IERC20 token = IERC20(tokens[i]);
            uint256 balance = token.balanceOf(address(this));
            uint256 price = getTokenPrice(tokens[i]);
            values[i] = balance.mul(price);
            unchecked {
                totalValue = totalValue.add(values[i]);
            }
        }

        uint256 tokenCount = tokens.length;
        require(tokenCount > 0, "No tokens set for the portfolio");

        uint256 targetValue = totalValue.div(tokenCount);

        for (uint256 i = 0; i < tokenCount; i++) {
            IERC20 token = IERC20(tokens[i]);
            uint256 balance = token.balanceOf(address(this));
            uint256 price = getTokenPrice(tokens[i]);
            uint256 currentValue = balance.mul(price);

            if (currentValue > targetValue) {
                uint256 excessValue = currentValue.sub(targetValue);
                uint256 excessTokens = excessValue.div(price);
                token.safeApprove(address(uniswapV2Router), excessTokens);
                token.safeApprove(address(uniswapV3Router), excessTokens);
                swapTokenForETH(tokens[i], excessTokens);
            } else if (currentValue < targetValue) {
                uint256 deficitValue = targetValue.sub(currentValue);
                uint256 ethAmount = deficitValue.div(price);
                swapETHForToken(tokens[i], ethAmount);
            }
        }

        emit Rebalanced(msg.sender, block.timestamp);
    }

    function processTradeQueue(uint256 batchSize) external onlyRole(MANAGER_ROLE) {
        uint256 length = tradeQueue.length;
        if (batchSize > length) {
            batchSize = length;
        }
        uint256 count = 0;
        for (uint256 i = 0; i < length && count < batchSize; i++) {
            Trade storage trade = tradeQueue[i];
            if (block.timestamp >= trade.nextRetryTime) {
                if (trade.isHighGas && getCurrentGasPrice() > gasPriceThreshold) {
                    trade.nextRetryTime = block.timestamp + 1 hours;
                    continue;
                }
                if (trade.isSlippage && !tryTrade(trade.user, trade.tokenIn, trade.tokenOut, trade.amountIn)) {
                    trade.retryCounter++;
                    if (trade.retryCounter >= maxRetryAttempts) {
                        emit TradeFailed(trade.user, trade.tokenIn, trade.tokenOut, trade.amountIn, trade.tradeType, "Max retry attempts reached");
                        removeTrade(i);
                        i--; // Adjust index after removal
                        length--;
                    } else {
                        trade.nextRetryTime = block.timestamp + 1 hours;
                    }
                    continue;
                }
                // Execute trade
                if (keccak256(bytes(trade.tradeType)) == keccak256("deposit")) {
                    executeDepositETH(trade.user, trade.amountIn);
                } else if (keccak256(bytes(trade.tradeType)) == keccak256("rebalance")) {
                    executeRebalance();
                } else if (keccak256(bytes(trade.tradeType)) == keccak256("liquidation")) {
                    swapTokenForETH(trade.tokenIn, trade.amountIn);
                }
                emit TradeProcessed(trade.user, trade.tokenIn, trade.tokenOut, trade.amountIn, trade.tradeType);
                removeTrade(i);
                i--; // Adjust index after removal
                length--;
                count++;
            }
        }
    }

    function tryTrade(address user, address tokenIn, address tokenOut, uint256 amountIn) internal returns (bool) {
        (bool useV2, uint256 amountOut) = getBestSwapOption(amountIn, tokenOut, tokenIn);
        uint256 minAmountOut = amountOut.mul(uint256(10000).sub(slippageTolerance)).div(10000);

        if (useV2) {
            address[] memory path = new address[](2);
            path[0] = tokenIn == address(0) ? uniswapV2Router.WETH() : tokenIn;
            path[1] = tokenOut;
            uint256 deadline = block.timestamp + 15;

            try uniswapV2Router.swapExactETHForTokensSupportingFeeOnTransferTokens{value: amountIn}(
                minAmountOut,
                path,
                address(this),
                deadline
            ) {
                return true;
            } catch {
                return false;
            }
        } else {
            ISwapRouter.ExactInputSingleParams memory params = ISwapRouter.ExactInputSingleParams({
                tokenIn: tokenIn == address(0) ? uniswapV2Router.WETH() : tokenIn,
                tokenOut: tokenOut,
                fee: 3000,
                recipient: address(this),
                deadline: block.timestamp + 15,
                amountIn: amountIn,
                amountOutMinimum: minAmountOut,
                sqrtPriceLimitX96: 0
            });

            try uniswapV3Router.exactInputSingle{value: amountIn}(params) {
                return true;
            } catch {
                return false;
            }
        }
    }

    function removeTrade(uint256 index) internal {
        tradeQueue[index] = tradeQueue[tradeQueue.length - 1];
        tradeQueue.pop();
    }

    function swapTokenForETH(address token, uint256 tokenAmount) internal {
        (bool useV2, uint256 amountOut) = getBestSwapOption(tokenAmount, address(0), token);
        uint256 minAmountOut = amountOut.mul(uint256(10000).sub(slippageTolerance)).div(10000);

        if (useV2) {
            address[] memory path = new address[](2);
            path[0] = token;
            path[1] = uniswapV2Router.WETH();

            uint256 deadline = block.timestamp + 15;

            try uniswapV2Router.swapExactTokensForETHSupportingFeeOnTransferTokens(
                tokenAmount,
                minAmountOut,
                path,
                address(this),
                deadline
            ) {
                emit TokenSwapped(token, tokenAmount, amountOut);
            } catch {
                tradeQueue.push(Trade(msg.sender, token, uniswapV2Router.WETH(), tokenAmount, "liquidation", 0, block.timestamp + 1 hours, false, true));
                emit TradeQueued(msg.sender, token, uniswapV2Router.WETH(), tokenAmount, "liquidation");
            }
        } else {
            ISwapRouter.ExactInputSingleParams memory params = ISwapRouter.ExactInputSingleParams({
                tokenIn: token,
                tokenOut: uniswapV2Router.WETH(),
                fee: 3000,
                recipient: address(this),
                deadline: block.timestamp + 15,
                amountIn: tokenAmount,
                amountOutMinimum: minAmountOut,
                sqrtPriceLimitX96: 0
            });

            try uniswapV3Router.exactInputSingle(params) {
                emit TokenSwapped(token, tokenAmount, amountOut);
            } catch {
                tradeQueue.push(Trade(msg.sender, token, uniswapV2Router.WETH(), tokenAmount, "liquidation", 0, block.timestamp + 1 hours, false, true));
                emit TradeQueued(msg.sender, token, uniswapV2Router.WETH(), tokenAmount, "liquidation");
            }
        }
    }

    function swapETHForToken(address token, uint256 ethAmount) internal {
        (bool useV2, uint256 amountOut) = getBestSwapOption(ethAmount, token, address(0));
        uint256 minAmountOut = amountOut.mul(uint256(10000).sub(slippageTolerance)).div(10000);

        if (useV2) {
            address[] memory path = new address[](2);
            path[0] = uniswapV2Router.WETH();
            path[1] = token;

            uint256 deadline = block.timestamp + 15;

            try uniswapV2Router.swapExactETHForTokensSupportingFeeOnTransferTokens{value: ethAmount}(
                minAmountOut,
                path,
                address(this),
                deadline
            ) {
                emit TokenSwapped(token, ethAmount, amountOut);
            } catch {
                tradeQueue.push(Trade(msg.sender, uniswapV2Router.WETH(), token, ethAmount, "rebalance", 0, block.timestamp + 1 hours, false, true));
                emit TradeQueued(msg.sender, uniswapV2Router.WETH(), token, ethAmount, "rebalance");
            }
        } else {
            ISwapRouter.ExactInputSingleParams memory params = ISwapRouter.ExactInputSingleParams({
                tokenIn: uniswapV2Router.WETH(),
                tokenOut: token,
                fee: 3000,
                recipient: address(this),
                deadline: block.timestamp + 15,
                amountIn: ethAmount,
                amountOutMinimum: minAmountOut,
                sqrtPriceLimitX96: 0
            });

            try uniswapV3Router.exactInputSingle{value: ethAmount}(params) {
                emit TokenSwapped(token, ethAmount, amountOut);
            } catch {
                tradeQueue.push(Trade(msg.sender, uniswapV2Router.WETH(), token, ethAmount, "rebalance", 0, block.timestamp + 1 hours, false, true));
                emit TradeQueued(msg.sender, uniswapV2Router.WETH(), token, ethAmount, "rebalance");
            }
        }
    }

    function getBestSwapOption(uint256 amountIn, address tokenOut, address tokenIn) internal view returns (bool useV2, uint256 amountOut) {
        // Check Uniswap V2
        address[] memory path = new address[](2);
        path[0] = tokenIn == address(0) ? uniswapV2Router.WETH() : tokenIn;
        path[1] = tokenOut;
        uint256[] memory amountsOutV2 = uniswapV2Router.getAmountsOut(amountIn, path);

        // Check Uniswap V3
        uint256 amountOutV3;
        try uniswapV3Router.exactInputSingle(
            ISwapRouter.ExactInputSingleParams({
                tokenIn: tokenIn == address(0) ? uniswapV2Router.WETH() : tokenIn,
                tokenOut: tokenOut,
                fee: 3000,
                recipient: address(this),
                deadline: block.timestamp,
                amountIn: amountIn,
                amountOutMinimum: 0,
                sqrtPriceLimitX96: 0
            })
        ) returns (uint256 amountOut) {
            amountOutV3 = amountOut;
        } catch {
            amountOutV3 = 0;
        }

        // Compare amounts and select the best option
        if (amountsOutV2[1] >= amountOutV3) {
            return (true, amountsOutV2[1]);
        } else {
            return (false, amountOutV3);
        }
    }

    function getTokenPrice(address token) internal view returns (uint256) {
        // Try to fetch price from Uniswap V3
        address poolAddress = uniswapV3Factory.getPool(token, uniswapV2Router.WETH(), 3000);
        if (poolAddress != address(0)) {
            IUniswapV3Pool pool = IUniswapV3Pool(poolAddress);
            (uint160 sqrtPriceX96,,,,,,) = pool.slot0();
            uint256 price = uint256(sqrtPriceX96) * uint256(sqrtPriceX96) * 1e18 >> (96 * 2);
            return price;
        }

        // If Uniswap V3 pool doesn't exist, fetch price from Uniswap V2
        address[] memory path = new address[](2);
        path[0] = token;
        path[1] = uniswapV2Router.WETH();
        uint256[] memory amountsOut = uniswapV2Router.getAmountsOut(1e18, path);
        return amountsOut[1];
    }

    function withdrawETH(uint256 amount) external nonReentrant onlyWhitelisted whenNotPaused {
        require(amount > 0, "Withdraw amount must be greater than zero");
        uint256 totalUserBalance = userBalances[msg.sender].eth;
        require(totalUserBalance > 0 && totalUserBalance >= amount, "Insufficient ETH balance");

        uint256 fee = 0;
        if (msg.sender != owner()) {
            unchecked {
                fee = amount.mul(withdrawalFee).div(10000);
                accumulatedETHFees = accumulatedETHFees.add(fee);
            }
        }
        uint256 netAmount = amount.sub(fee);
        userBalances[msg.sender].eth = userBalances[msg.sender].eth.sub(amount);
        payable(msg.sender).transfer(netAmount);
        emit Withdrawn(msg.sender, netAmount, "ETH");
    }

    function totalETHBalance() public view returns (uint256) {
        return address(this).balance;
    }

    function ownerWithdrawETH(uint256 amount) external onlyOwner {
        require(address(this).balance >= amount, "Insufficient ETH balance");
        require(address(this).balance - amount >= minimumGasETH, "Cannot withdraw below minimum GasETH");
        payable(owner()).transfer(amount);
        emit GasETHWithdrawn(owner(), amount);
    }

    function withdrawAccumulatedFees() external onlyOwner nonReentrant {
        if (accumulatedETHFees > 0) {
            uint256 ethFees = accumulatedETHFees;
            accumulatedETHFees = 0;
            payable(owner()).transfer(ethFees);
            emit FeesWithdrawn(owner(), ethFees, "ETH");
        }
    }

    receive() external payable onlyWhitelisted whenNotPaused {
        require(msg.value > 0, "ETH amount must be greater than zero");

        if (getCurrentGasPrice() > gasPriceThreshold) {
            tradeQueue.push(Trade(msg.sender, address(0), address(0), msg.value, "deposit", 0, block.timestamp + 1 hours, true, false));
            emit TradeQueued(msg.sender, address(0), address(0), msg.value, "deposit");
        } else {
            executeDepositETH(msg.sender, msg.value);
        }
    }

    fallback() external payable onlyWhitelisted whenNotPaused {
        require(msg.value > 0, "ETH amount must be greater than zero");

        if (getCurrentGasPrice() > gasPriceThreshold) {
            tradeQueue.push(Trade(msg.sender, address(0), address(0), msg.value, "deposit", 0, block.timestamp + 1 hours, true, false));
            emit TradeQueued(msg.sender, address(0), address(0), msg.value, "deposit");
        } else {
            executeDepositETH(msg.sender, msg.value);
        }
    }

    function liquidate() external onlyOwner nonReentrant whenNotPaused {
        uint256 totalEth = address(this).balance;

        // Swap all tokens to ETH
        for (uint256 i = 0; i < tokens.length; i++) {
            IERC20 token = IERC20(tokens[i]);
            uint256 tokenBalance = token.balanceOf(address(this));
            if (tokenBalance > 0) {
                swapTokenForETH(tokens[i], tokenBalance);
            }
        }

        uint256 remainingEth = address(this).balance;
        uint256 ownerBalance = userBalances[owner()].eth;
        uint256 totalUserDeposits = 0;

        // Calculate total user deposits and total balance
        for (uint256 i = 0; i < tokens.length; i++) {
            totalUserDeposits = totalUserDeposits.add(userBalances[tokens[i]].eth);
        }

        totalUserDeposits = totalUserDeposits.add(ownerBalance);

        // Distribute ETH to users and owner based on their share
        for (uint256 i = 0; i < tokens.length; i++) {
            address user = tokens[i];
            uint256 userBalance = userBalances[user].eth;
            if (userBalance > 0) {
                uint256 userShare = remainingEth.mul(userBalance).div(totalUserDeposits);
                payable(user).transfer(userShare);
                userBalances[user].eth = 0;
            }
        }

        // Distribute ETH to owner based on their share and accumulated fees
        uint256 ownerShare = remainingEth.mul(ownerBalance).div(totalUserDeposits);
        uint256 ownerEth = ownerShare.add(accumulatedETHFees);

        // Transfer owner's share to owner
        if (ownerEth > 0) {
            payable(owner()).transfer(ownerEth);
        }

        accumulatedETHFees = 0;

        emit Liquidated(remainingEth);
    }

    function withdrawRemainingTokens(address token) external onlyOwner {
        uint256 tokenBalance = IERC20(token).balanceOf(address(this));
        require(tokenBalance > 0, "No tokens to withdraw");
        IERC20(token).safeTransfer(owner(), tokenBalance);
    }

    function withdrawRemainingETH() external onlyOwner {
        uint256 ethBalance = address(this).balance;
        require(ethBalance > minimumGasETH, "Insufficient ETH balance after maintaining GasETH");
        uint256 withdrawAmount = ethBalance.sub(minimumGasETH);
        require(withdrawAmount > 0, "No ETH to withdraw after maintaining GasETH");
        payable(owner()).transfer(withdrawAmount);
    }

    // Function to withdraw proportional holdings in the portfolio
    function withdrawInKind() external nonReentrant onlyWhitelisted whenNotPaused {
        // Retrieve user's address
        address user = msg.sender;

        // Retrieve user's total ETH balance in the portfolio
        uint256 userEthBalance = userBalances[user].eth;
        require(userEthBalance > 0, "No balance to withdraw");

        // Calculate the total portfolio value
        uint256 totalPortfolioValue = 0;
        for (uint256 i = 0; i < tokens.length; i++) {
            IERC20 token = IERC20(tokens[i]);
            uint256 tokenBalance = token.balanceOf(address(this));
            uint256 tokenValue = tokenBalance.mul(getTokenPrice(tokens[i]));
            totalPortfolioValue = totalPortfolioValue.add(tokenValue);
        }

        // Calculate the user's share of the portfolio
        uint256 userShare = userEthBalance.mul(1e18).div(totalETHBalance());

        // Calculate the withdrawal fee in ETH terms
        uint256 fee = userEthBalance.mul(withdrawalFee).div(10000);
        accumulatedETHFees = accumulatedETHFees.add(fee);
        uint256 netUserEthBalance = userEthBalance.sub(fee);

        // Transfer the user's proportional share of each token after covering the withdrawal fee
        for (uint256 i = 0; i < tokens.length; i++) {
            IERC20 token = IERC20(tokens[i]);
            uint256 tokenBalance = token.balanceOf(address(this));
            uint256 amountToTransfer = tokenBalance.mul(userShare).div(1e18);
            if (amountToTransfer > 0) {
                // Swap tokens to cover the fee in chronological order
                uint256 tokenFeeAmount = amountToTransfer.mul(fee).div(userEthBalance);
                if (tokenFeeAmount > 0) {
                    swapTokenForETH(tokens[i], tokenFeeAmount);
                }
                uint256 remainingTokenAmount = amountToTransfer.sub(tokenFeeAmount);
                if (remainingTokenAmount > 0) {
                    token.safeTransfer(user, remainingTokenAmount);
                }
            }
        }

        // Update user's ETH balance in the contract
        userBalances[user].eth = 0;
    }

    // Emergency function to withdraw all ETH from the contract
    function emergencyWithdrawETH() external onlyOwner nonReentrant {
        uint256 ethBalance = address(this).balance;
        require(ethBalance > 0, "No ETH to withdraw");
        uint256 gasReserve = 0.01 ether; // Reserve some ETH to cover gas costs
        if (ethBalance > gasReserve) {
            uint256 withdrawAmount = ethBalance.sub(gasReserve);
            payable(owner()).transfer(withdrawAmount);
            emit EmergencyWithdrawn(owner(), withdrawAmount, "ETH");
        } else {
            // Transfer all ETH if balance is less than the gas reserve
            payable(owner()).transfer(ethBalance);
            emit EmergencyWithdrawn(owner(), ethBalance, "ETH");
        }
    }

    // Emergency function to withdraw all tokens from the contract
    function emergencyWithdrawTokens() external onlyOwner nonReentrant {
        for (uint256 i = 0; i < tokens.length; i++) {
            IERC20 token = IERC20(tokens[i]);
            uint256 tokenBalance = token.balanceOf(address(this));
            if (tokenBalance > 0) {
                token.safeTransfer(owner(), tokenBalance);
                emit EmergencyTokenWithdrawn(owner(), address(token), tokenBalance);
            }
        }
    }
}
