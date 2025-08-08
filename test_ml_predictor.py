from ml_predictor import MLPredictor


def test_train_predict_evaluate_and_optimize():
    X = [[0.0], [1.0], [0.2], [0.8]]
    y = [0, 1, 0, 1]
    predictor = MLPredictor()
    predictor.train_model(X, y)
    pred = predictor.predict_signal([0.9])
    assert pred in (0, 1)
    acc = predictor.evaluate_model(X, y)
    assert 0.0 <= acc <= 1.0
    predictor.optimize_model(X, y, param_grid={"C": [0.1, 1.0]})
    acc2 = predictor.evaluate_model(X, y)
    assert 0.0 <= acc2 <= 1.0
    predictor.log_model_results({"accuracy": acc2})
