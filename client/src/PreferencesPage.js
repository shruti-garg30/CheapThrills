import React, { useEffect, useState } from "react";
import { useNavigate, useLocation } from "react-router-dom";
import Select from 'react-select'
import LoginPage from "./LoginPage";
import gif from './assets/giphy.gif';


const PreferencesPage = (props) => {
  const [originCity, setOriginCity]= useState("");
  const [budget, setBudget] = useState("");
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const navigate = useNavigate();
  const location = useLocation();
  const name = location.state?.name;
  const email = location.state?.email;
  const userdata = location.state?.data;
  const [res, setRes] = useState([]);
  const [errors, setErrors] = useState({ startDate: "" , endDate: "" });
  const [options, setOptions] = useState([]);
  const [loading, setLoading] = useState(false);
  const [calculating, setCalculating] = useState(false)

  useEffect(() => {
    setLoading(true);
    async function getCities() {
    const result = await fetch(`http://localhost:8000/get-cities`, {'method': 'GET'})
    const result_json = await result.json()
    console.log(result_json)
    setOptions(result_json)
    setLoading(false);
    }
    getCities();
  }, [])
  

  const validateStartDate = (sD) => {
    const currentDate = new Date();
    const start = new Date(sD);
    if(start < currentDate){
      return false;
    }

    return true;

  }

  const validateEndDate = (sD, eD) => {
    const currentDate = new Date();
    const start = new Date(sD);
    const end = new Date(eD);

    if (start < currentDate || end <= start) {
      return false;
    }

    return true;
  }

  const handleSubmit = (e) => {
    e.preventDefault();

    let startError = "";
        let endError = "";

        if (!validateStartDate(startDate)) {
            startError = "Please enter a valid start date.";
        }

        if (!validateEndDate(endDate)) {
            endError = "Please enter a valid end date.";
        }

        if (startError || endError) {
            setErrors({ startDate: startError, endDate: endError });
            return;
        }


    const user = {
      name,
      email,
      originCity,
      budget,
      startDate,
      endDate,
      location
    };

  // console.log(user)
  
  setCalculating(true)
  fetch(`http://localhost:8000/send_preferences`, {
                'method': 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(user)
            })
            .then(
              async response => {
              if(response.ok){
                const data = await response.json();
                setRes(data)
                console.log(data["past_search"])
                return data;
              }
              else {
                console.log(response.statusText);
              }
            })
            .then((data) => {
              setCalculating(false)
              navigate("/dashboard", { state: { user, data} }) })
            .catch(error => console.log(error));

  };

  // const options = [
  //   { value: 'New York City', label: 'New York City' },
  //   { value: 'Los Angeles', label: 'Los Angeles' },
  //   { value: 'Las Vegas', label: 'Las Vegas' }
  // ]
  
  const MyComponent = () => (
    <Select options={options} />
  )

  const style = {
    container: {
        background: "url('https://images.pexels.com/photos/1051075/pexels-photo-1051075.jpeg?auto=compress&cs=tinysrgb&w=1260&h=750&dpr=2') no-repeat center center fixed",
        backgroundSize: "cover",
        minHeight: "100vh",
        display: "flex",
        flexDirection: "column",
        justifyContent: "center",
        alignItems: "center",
        fontFamily: "'Roboto', sans-serif",
      },
      form: {
        backgroundColor: "rgba(255, 255, 255, 0.8)",
        padding: "30px",
        borderRadius: "10px",
        width: "400px",
        maxWidth: "100%",
        textAlign: "center",
      },
      title: {
        color: "#3a3a3a",
        marginBottom: "20px",
      },
      label: {
        display: "block",
        marginBottom: "5px",
      },
      input: {
        width: "100%",
        padding: "8px",
        marginBottom: "20px",
        borderRadius: "4px",
        border: "1px solid #ccc",
        fontSize: "14px",
        fontFamily: "inherit",
      },
      button: {
        width: "100%",
        padding: "10px",
        borderRadius: "4px",
        backgroundColor: "#66bb6a",
        border: "none",
        color: "white",
        fontSize: "16px",
        cursor: "pointer",
      },
      links: {
        marginTop: "20px",
        fontSize: "12px",
      },
      link: {
        color: "#3a3a3a",
        textDecoration: "none",
      },
  };

  return (
    <div>
    {userdata && userdata.name === 'Not Valid' ? (    <div style={{ 
      backgroundColor: '#fff', 
      border: '1px solid #ccc', 
      borderRadius: '4px', 
      boxShadow: '0 2px 4px rgba(0, 0, 0, 0.1)', 
      margin: '50px auto', 
      maxWidth: '500px', 
      padding: '30px', 
      textAlign: 'center' 
    }}>
      <h1 style={{ fontSize: '28px', marginBottom: '20px', color: 'red' }}>Error</h1>
      <p style={{ marginBottom: '20px', fontWeight: "bold" }}>Username and email do not match.</p>
      <button 
        style={{ 
          backgroundColor: '#333', 
          border: 'none', 
          borderRadius: '4px', 
          color: '#fff', 
          cursor: 'pointer', 
          fontSize: '16px', 
          padding: '10px 20px', 
          transition: 'background-color 0.2s ease-in-out' 
        }} 
        onClick={() => { window.location.href = '/' }}
      >
        Go back
      </button>
    </div>): (<div>{
      loading ? <div>Loading...</div> :
      !calculating ? (
      <div style={style.container}>
      <form onSubmit={handleSubmit} style={style.form}>
      <h1 style={style.title}>Hi, {name}! Tell us about your travel preferences</h1>
      <label htmlFor="originCity" style={style.label}>
          Enter Origin City:
        </label>
        <Select options={options} defaultValue={options[0]} onChange={(e) => setOriginCity(e.value)}/>
        <br />
        <label htmlFor="budget" style={style.label}>
          Budget(In USD):
        </label>
        <input
          id="budget"
          type="number"
          value={budget}
          required= 'true'
          onChange={(e) => setBudget(e.target.value)}
          style={style.input}
          min="0"
          step="1"
          placeholder="Enter budget amount"
        />
        <br />
        <label htmlFor="startDate" style={style.label}>
          Start Date:
        </label>
        <input
          id="startDate"
          type="date"
          value={startDate}
          required= 'true'
          onChange={(e) => setStartDate(e.target.value)}
          style={style.input}
        />
        {errors.startDate && !calculating && <div style={{color: 'red', position : "relative", padding : "2%", marginTop: "-10px", marginBottom: "10px"}}>{errors.startDate}</div>}
        <br />
        <label htmlFor="endDate" style={style.label}>
          End Date:
        </label>
        <input
          id="endDate"
          type="date"
          value={endDate}
          required= 'true'
          onChange={(e) => setEndDate(e.target.value)}
          style={style.input}
        />
        {errors.endDate && !calculating && <div style={{color: 'red', position : "relative", padding : "2%", marginTop: "-10px", marginBottom: "10px"}}>{errors.endDate}</div>}
        <br />
        {/* Add more preference fields as needed */}
        <button type="submit" style={style.button}>
          Step 2/4
        </button>
      </form>
    </div>) : (
            <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100vh' }}>
            <div style={{ fontSize: 'large', padding: '20px' }}>
            <img src={gif} alt="loading gif" />
              <p style={{ textAlign: 'center' }}>Calculating recommendations...</p>
              <p style={{ textAlign: 'center' }}>Fun Fact: You are a broke college student.</p>
              <p style={{ textAlign: 'center' }}>Feeling attacked?</p>
              <p style={{ textAlign: 'center' }}>Sounds like a YOU PROBLEM!</p>
            </div>
          </div>
    )}
    </div>
    ) }
  
    </div>)
    
};

export default PreferencesPage;
